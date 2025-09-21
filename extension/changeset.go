package extension

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/walterwanderley/sqlite"
)

type ChangeSet struct {
	Node      string   `json:"node"`
	Changes   []Change `json:"changes"`
	Timestamp int64    `json:"timestamp_ns"`
}

type Change struct {
	Database  string   `json:"database"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	Operation string   `json:"operation"` // "INSERT", "UPDATE", "DELETE"
	OldRowID  int64    `json:"old_rowid,omitempty"`
	NewRowID  int64    `json:"new_rowid,omitempty"`
	OldValues []any    `json:"old_values,omitempty"`
	NewValues []any    `json:"new_values,omitempty"`
}

func (cs *ChangeSet) Apply(conn *sqlite.Conn) error {
	if len(cs.Changes) == 0 {
		return nil
	}

	err := cs.setTableColumns(conn)
	if err != nil {
		return err
	}

	err = conn.Exec("BEGIN", nil)
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
		default:
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to apply change on %q: %w", change.Table, err)
		}
	}

	return conn.Exec("COMMIT", nil)
}

func (cs *ChangeSet) setTableColumns(conn *sqlite.Conn) error {
	tableColumns := make(map[string][]string)
	tableTypes := make(map[string][]string)
	for i, change := range cs.Changes {
		if len(change.Columns) > 0 {
			continue
		}
		if _, ok := tableColumns[change.Table]; !ok {
			columns, types, err := getTableColumnsAndTypes(conn, change.Table)
			if err != nil {
				return fmt.Errorf("failed to get table columns from %q: %w", change.Table, err)
			}
			tableTypes[change.Table] = types
			tableColumns[change.Table] = columns
			cs.Changes[i].Columns = columns
		} else {
			cs.Changes[i].Columns = tableColumns[change.Table]
		}
		types := tableTypes[change.Table]
		for j, ctype := range types {
			if strings.Contains(ctype, "TEXT") || strings.Contains(ctype, "CHAR") ||
				strings.Contains(ctype, "CLOB") || strings.Contains(ctype, "JSON") ||
				strings.Contains(ctype, "DATE") || strings.Contains(ctype, "TIME") {
				if len(change.OldValues) > 0 && j < len(change.OldValues) {
					change.OldValues[j] = convert(change.OldValues[j])
				}
				if len(change.NewValues) > 0 && j < len(change.NewValues) {
					change.NewValues[j] = convert(change.NewValues[j])
				}
			}
		}
	}
	return nil
}

func getTableColumnsAndTypes(conn *sqlite.Conn, table string) ([]string, []string, error) {
	var columns, types []string
	err := conn.Exec("SELECT name, type FROM PRAGMA_table_info(?)", func(stmt *sqlite.Stmt) error {
		columns = append(columns, stmt.GetText("name"))
		types = append(types, stmt.GetText("type"))
		return nil
	}, table)

	if err != nil {
		return nil, nil, err
	}
	return columns, types, nil
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

func convert(src any) string {
	switch v := src.(type) {
	case []byte:
		var dst []byte
		n, err := base64.StdEncoding.Decode(dst, v)
		if err != nil {
			return string(v)
		}
		return string(dst[0:n])
	case string:
		dst, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return v
		}
		return string(dst)
	default:
		return fmt.Sprintf("%s", src)
	}

}
