package sql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/thirawat27/kvi/pkg/types"
	"github.com/xwb1989/sqlparser"
)

// Executor translates standard SQL ASTs into KVi engine operations.
// Supported statements: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE (no-op).
type Executor struct {
	engine types.Engine
}

func NewExecutor(e types.Engine) *Executor {
	return &Executor{engine: e}
}

// ExecuteQuery parses a 100 % standard SQL string and maps it to KVi operations.
func (xe *Executor) ExecuteQuery(ctx context.Context, query string) (interface{}, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}

	switch ast := stmt.(type) {
	case *sqlparser.Select:
		return xe.handleSelect(ctx, ast)
	case *sqlparser.Insert:
		return xe.handleInsert(ctx, ast)
	case *sqlparser.Update:
		return xe.handleUpdate(ctx, ast)
	case *sqlparser.Delete:
		return xe.handleDelete(ctx, ast)
	case *sqlparser.DDL:
		// CREATE TABLE, DROP TABLE – accepted as no-ops (schema-free KV store)
		return map[string]string{"status": "ok", "note": "schema statements are no-ops in Kvi"}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type %T; Kvi supports SELECT / INSERT / UPDATE / DELETE", stmt)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// extractIDFromWhere pulls the primary-key value from a WHERE id = '...' clause.
func (xe *Executor) extractIDFromWhere(where *sqlparser.Where) (string, error) {
	if where == nil {
		return "", errors.New("WHERE clause is required (must specify id = 'value')")
	}
	return xe.exprToID(where.Expr)
}

func (xe *Executor) exprToID(expr sqlparser.Expr) (string, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != "=" {
			return "", fmt.Errorf("only '=' is supported in WHERE; got '%s'", e.Operator)
		}
		col, ok := e.Left.(*sqlparser.ColName)
		if !ok {
			return "", errors.New("left side of WHERE must be a column name")
		}
		if strings.ToLower(col.Name.String()) != "id" {
			return "", errors.New("Kvi primary-key column is 'id'; only WHERE id = '...' is supported")
		}
		val, ok := e.Right.(*sqlparser.SQLVal)
		if !ok {
			return "", errors.New("right side of WHERE must be a literal value")
		}
		return string(val.Val), nil

	case *sqlparser.AndExpr:
		// For compound WHERE, attempt to find the id clause on left
		id, err := xe.exprToID(e.Left)
		if err == nil {
			return id, nil
		}
		return xe.exprToID(e.Right)

	default:
		return "", fmt.Errorf("unsupported WHERE expression type %T", expr)
	}
}

// sqlValToGo converts a *sqlparser.SQLVal to its natural Go type.
func sqlValToGo(v *sqlparser.SQLVal) interface{} {
	s := string(v.Val)
	switch v.Type {
	case sqlparser.IntVal:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	case sqlparser.FloatVal:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	// Default to string (covers StrVal, HexVal, etc.)
	return s
}

// ── SELECT ───────────────────────────────────────────────────────────────────

func (xe *Executor) handleSelect(ctx context.Context, stmt *sqlparser.Select) (interface{}, error) {
	id, err := xe.extractIDFromWhere(stmt.Where)
	if err != nil {
		return nil, err
	}
	return xe.engine.Get(ctx, id)
}

// ── INSERT ───────────────────────────────────────────────────────────────────

func (xe *Executor) handleInsert(ctx context.Context, stmt *sqlparser.Insert) (interface{}, error) {
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(rows) == 0 {
		return nil, errors.New("INSERT must include a VALUES clause")
	}

	var results []map[string]string
	for _, tuple := range rows {
		if len(stmt.Columns) != len(tuple) {
			return nil, fmt.Errorf("column count (%d) does not match values count (%d)",
				len(stmt.Columns), len(tuple))
		}

		var id string
		data := make(map[string]interface{})

		for i, col := range stmt.Columns {
			colName := strings.ToLower(col.String())
			valExpr := tuple[i]

			var goVal interface{}
			switch v := valExpr.(type) {
			case *sqlparser.SQLVal:
				goVal = sqlValToGo(v)
			case *sqlparser.NullVal:
				goVal = nil
			default:
				return nil, fmt.Errorf("unsupported value expression %T in INSERT", valExpr)
			}

			if colName == "id" {
				id = fmt.Sprintf("%v", goVal)
			} else {
				data[colName] = goVal
			}
		}

		if id == "" {
			return nil, errors.New("INSERT must include an 'id' column as the primary key")
		}

		if err := xe.engine.Put(ctx, id, &types.Record{ID: id, Data: data}); err != nil {
			return nil, err
		}
		results = append(results, map[string]string{"status": "ok", "inserted_id": id})
	}

	if len(results) == 1 {
		return results[0], nil
	}
	return results, nil
}

// ── UPDATE ───────────────────────────────────────────────────────────────────

func (xe *Executor) handleUpdate(ctx context.Context, stmt *sqlparser.Update) (interface{}, error) {
	id, err := xe.extractIDFromWhere(stmt.Where)
	if err != nil {
		return nil, err
	}

	rec, err := xe.engine.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("record '%s' not found: %w", id, err)
	}

	for _, expr := range stmt.Exprs {
		colName := strings.ToLower(expr.Name.Name.String())
		switch v := expr.Expr.(type) {
		case *sqlparser.SQLVal:
			rec.Data[colName] = sqlValToGo(v)
		case *sqlparser.NullVal:
			rec.Data[colName] = nil
		default:
			return nil, fmt.Errorf("unsupported value type %T in UPDATE SET", expr.Expr)
		}
	}

	if err := xe.engine.Put(ctx, id, rec); err != nil {
		return nil, err
	}
	return map[string]string{"status": "ok", "updated_id": id}, nil
}

// ── DELETE ───────────────────────────────────────────────────────────────────

func (xe *Executor) handleDelete(ctx context.Context, stmt *sqlparser.Delete) (interface{}, error) {
	id, err := xe.extractIDFromWhere(stmt.Where)
	if err != nil {
		return nil, err
	}
	if err := xe.engine.Delete(ctx, id); err != nil {
		return nil, err
	}
	return map[string]string{"status": "ok", "deleted_id": id}, nil
}
