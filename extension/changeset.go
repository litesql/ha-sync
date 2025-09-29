package extension

import (
	"fmt"
	"strings"

	"github.com/walterwanderley/sqlite"
)

type ChangeSet struct {
	Node      string   `json:"node"`
	ProcessID int64    `json:"process_id"`
	Changes   []Change `json:"changes"`
	Timestamp int64    `json:"timestamp_ns"`
	StreamSeq uint64   `json:"-"`
}

type Change struct {
	Database  string   `json:"database,omitempty"`
	Table     string   `json:"table,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	Operation string   `json:"operation"` // "INSERT", "UPDATE", "DELETE", "SQL"
	OldRowID  int64    `json:"old_rowid,omitempty"`
	NewRowID  int64    `json:"new_rowid,omitempty"`
	OldValues []any    `json:"old_values,omitempty"`
	NewValues []any    `json:"new_values,omitempty"`
	SQL       string   `json:"sql,omitempty"`
	SQLArgs   []any    `json:"sql_args,omitempty"`
}

func (cs *ChangeSet) Apply(conn *sqlite.Conn) error {
	if len(cs.Changes) == 0 {
		return nil
	}

	err := conn.Exec("BEGIN", nil)
	if err != nil {
		return err
	}
	defer conn.Exec("ROLLBACK", nil)
	for _, change := range cs.Changes {
		var sql string
		switch change.Operation {
		case "INSERT":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?%d", col, i+1)
			}
			sql = fmt.Sprintf("INSERT INTO %s.%s (%s, rowid) VALUES (%s) ON CONFLICT (rowid) DO UPDATE SET %s", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)+1), strings.Join(setClause, ", "))
			err = conn.Exec(sql, nil, append(change.NewValues, change.NewRowID)...)
		case "UPDATE":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE rowid = ?", change.Database, change.Table, strings.Join(setClause, ", "))
			args := append(change.NewValues, change.OldRowID)
			err = conn.Exec(sql, nil, args...)
		case "DELETE":
			sql = fmt.Sprintf("DELETE FROM %s.%s WHERE rowid = ?", change.Database, change.Table)
			err = conn.Exec(sql, nil, change.OldRowID)
		case "SQL":
			sql = change.SQL
			err = conn.Exec(change.SQL, nil, change.SQLArgs...)
		default:
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to exec %q: %w", sql, err)
		}
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
