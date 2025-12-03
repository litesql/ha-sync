package extension

import (
	"fmt"
	"strings"
	"time"

	"github.com/walterwanderley/sqlite"
)

type ChangeSet struct {
	Node      string   `json:"node"`
	ProcessID int64    `json:"process_id"`
	Changes   []Change `json:"changes"`
	Timestamp int64    `json:"timestamp_ns"`
	StreamSeq uint64   `json:"-"`
	Subject   string   `json:"-"`
}

type Change struct {
	Database  string   `json:"database,omitempty"`
	Table     string   `json:"table,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	PKColumns []string `json:"pk_columns,omitempty"`
	Operation string   `json:"operation"` // "INSERT", "UPDATE", "DELETE", "SQL"
	OldRowID  int64    `json:"old_rowid,omitempty"`
	NewRowID  int64    `json:"new_rowid,omitempty"`
	OldValues []any    `json:"old_values,omitempty"`
	NewValues []any    `json:"new_values,omitempty"`
	Command   string   `json:"command,omitempty"`
	Args      []any    `json:"args,omitempty"`
}

func (c Change) PKColumnsNames() []string {
	if len(c.PKColumns) == 0 {
		return []string{"rowid"}
	}
	return c.PKColumns
}

func (c Change) PKOldValues() []any {
	if len(c.PKColumns) == 0 {
		return []any{c.OldRowID}
	}
	pkValues := make([]any, len(c.PKColumns))
	pkIndex := 0
	for i, col := range c.Columns {
		if col == c.PKColumns[pkIndex] {
			pkValues[pkIndex] = c.OldValues[i]
			pkIndex++
			if pkIndex >= len(c.PKColumns) {
				break
			}
		}
	}
	return pkValues
}

func (cs *ChangeSet) Apply(conn *sqlite.Conn, rowStrategy string) error {
	if len(cs.Changes) == 0 {
		return nil
	}

	err := conn.Exec("BEGIN", nil)
	if err != nil {
		return err
	}
	defer conn.Exec("ROLLBACK", nil)
	switch rowStrategy {
	case "rowid":
		for _, change := range cs.Changes {
			var (
				err error
				sql string
			)
			switch change.Operation {
			case "INSERT":
				setClause := make([]string, len(change.Columns))
				for i, col := range change.Columns {
					setClause[i] = fmt.Sprintf("%s = ?%d", col, i+1)
				}
				sql = fmt.Sprintf("INSERT INTO %s.%s (%s, rowid) VALUES (%s) ON CONFLICT DO UPDATE SET %s, rowid = ?%d;", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)+1), strings.Join(setClause, ", "), len(change.NewValues)+1)
				err = conn.Exec(sql, nil, append(change.NewValues, change.NewRowID)...)
			case "UPDATE":
				setClause := make([]string, len(change.Columns))
				for i, col := range change.Columns {
					setClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE rowid = ?;", change.Database, change.Table, strings.Join(setClause, ", "))
				args := append(change.NewValues, change.OldRowID)
				err = conn.Exec(sql, nil, args...)
			case "DELETE":
				sql = fmt.Sprintf("DELETE FROM %s.%s WHERE rowid = ?;", change.Database, change.Table)
				err = conn.Exec(sql, nil, change.OldRowID)
			case "SQL":
				sql = change.Command
				err = conn.Exec(sql, nil, change.Args...)
			default:
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to exec %q: %w", sql, err)
			}
		}
	case "full":
		for _, change := range cs.Changes {
			var (
				err error
				sql string
			)
			switch change.Operation {
			case "INSERT":
				sql = fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)))
				err = conn.Exec(sql, nil, change.NewValues...)
			case "UPDATE":
				setClause := make([]string, len(change.Columns))
				for i, col := range change.Columns {
					setClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", change.Database, change.Table, strings.Join(setClause, ", "), strings.Join(setClause, " AND "))
				args := append(change.NewValues, change.OldValues...)
				err = conn.Exec(sql, nil, args...)
			case "DELETE":
				whereClause := make([]string, len(change.Columns))
				for i, col := range change.Columns {
					whereClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", change.Database, change.Table, strings.Join(whereClause, " AND "))
				err = conn.Exec(sql, nil, change.OldValues...)
			case "SQL":
				sql = change.Command
				err = conn.Exec(sql, nil, change.Args...)
			default:
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to exec %q: %w", sql, err)
			}
		}
	default:
		for _, change := range cs.Changes {
			var (
				err error
				sql string
			)
			switch change.Operation {
			case "INSERT":
				sql = fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)))
				err = conn.Exec(sql, nil, change.NewValues...)
			case "UPDATE":
				setClause := make([]string, len(change.Columns))
				for i, col := range change.Columns {
					setClause[i] = fmt.Sprintf("%s = ?", col)
				}
				pkColumns := change.PKColumnsNames()
				whereClause := make([]string, len(pkColumns))
				for i, col := range pkColumns {
					whereClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", change.Database, change.Table, strings.Join(setClause, ", "), strings.Join(whereClause, " AND "))
				args := append(change.NewValues, change.PKOldValues()...)
				err = conn.Exec(sql, nil, args...)
			case "DELETE":
				pkColumns := change.PKColumnsNames()
				whereClause := make([]string, len(pkColumns))
				for i, col := range pkColumns {
					whereClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", change.Database, change.Table, strings.Join(whereClause, " AND "))
				err = conn.Exec(sql, nil, change.PKOldValues()...)
			case "SQL":
				sql = change.Command
				err = conn.Exec(sql, nil, change.Args...)
			default:
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to exec %q: %w", sql, err)
			}
		}
	}
	err = conn.Exec("REPLACE INTO ha_stats(subject, received_seq, updated_at) VALUES(?, ?, ?)", nil, cs.Subject, cs.StreamSeq, time.Now().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("failed to update ha_stats: %v", err)
	}

	return conn.Exec("COMMIT", nil)
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := range n {
		b.WriteString(fmt.Sprintf("?%d,", i+1))
	}
	return strings.TrimRight(b.String(), ",")
}
