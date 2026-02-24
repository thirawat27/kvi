package sql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thirawat27/kvi/pkg/types"
	"github.com/xwb1989/sqlparser"
)

type Executor struct {
	engine types.Engine
}

func NewExecutor(e types.Engine) *Executor {
	return &Executor{engine: e}
}

// ExecuteQuery receives a standard SQL query string, parses it using Vitess SQL parser,
// and maps standard relational SQL (SELECT, INSERT, UPDATE, DELETE) into KVi KV NoSQL Engine operations.
func (xe *Executor) ExecuteQuery(ctx context.Context, query string) (interface{}, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("SQL Parse Error: %v", err)
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
	default:
		return nil, fmt.Errorf("unsupported SQL command, Kvi engine only supports SELECT, INSERT, UPDATE, DELETE")
	}
}

func (xe *Executor) extractIDFromWhere(where *sqlparser.Where) (string, error) {
	if where == nil {
		return "", errors.New("WHERE clause is required (must specify id = '...')")
	}

	comp, ok := where.Expr.(*sqlparser.ComparisonExpr)
	if !ok || comp.Operator != "=" {
		return "", errors.New("only '=' operator is supported in WHERE clause right now for Key-Value access")
	}

	col, ok := comp.Left.(*sqlparser.ColName)
	if !ok {
		return "", errors.New("left side of WHERE must be a column name")
	}

	if strings.ToLower(col.Name.String()) != "id" {
		return "", errors.New("only 'id' column is supported for querying the primary key right now")
	}

	val, ok := comp.Right.(*sqlparser.SQLVal)
	if !ok {
		return "", errors.New("right side of WHERE must be a value")
	}

	return string(val.Val), nil
}

func (xe *Executor) handleSelect(ctx context.Context, stmt *sqlparser.Select) (interface{}, error) {
	id, err := xe.extractIDFromWhere(stmt.Where)
	if err != nil {
		return nil, err
	}

	rec, err := xe.engine.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (xe *Executor) handleInsert(ctx context.Context, stmt *sqlparser.Insert) (interface{}, error) {
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(rows) == 0 {
		return nil, errors.New("INSERT must have VALUES")
	}

	tuple := rows[0]

	if len(stmt.Columns) != len(tuple) {
		return nil, errors.New("column count doesn't match values count")
	}

	var id string
	data := make(map[string]interface{})

	for i, col := range stmt.Columns {
		colName := strings.ToLower(col.String())
		valExpr, ok := tuple[i].(*sqlparser.SQLVal)
		var val interface{}
		if ok {
			val = string(valExpr.Val)
		} else {
			return nil, errors.New("unsupported value type in INSERT")
		}

		if colName == "id" {
			id = val.(string)
		} else {
			data[colName] = val
		}
	}

	if id == "" {
		return nil, errors.New("INSERT must include 'id' column as the primary key")
	}

	rec := &types.Record{
		ID:   id,
		Data: data,
	}

	if err := xe.engine.Put(ctx, id, rec); err != nil {
		return nil, err
	}

	return map[string]string{"status": "ok", "inserted_id": id}, nil
}

func (xe *Executor) handleUpdate(ctx context.Context, stmt *sqlparser.Update) (interface{}, error) {
	id, err := xe.extractIDFromWhere(stmt.Where)
	if err != nil {
		return nil, err
	}

	rec, err := xe.engine.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("record not found: %v", err)
	}

	for _, expr := range stmt.Exprs {
		colName := strings.ToLower(expr.Name.Name.String())
		valExpr, ok := expr.Expr.(*sqlparser.SQLVal)
		if !ok {
			return nil, errors.New("unsupported value type in UPDATE SET")
		}
		// In KVi, Data is a dynamic map
		rec.Data[colName] = string(valExpr.Val)
	}

	if err := xe.engine.Put(ctx, id, rec); err != nil {
		return nil, err
	}

	return map[string]string{"status": "ok", "updated_id": id}, nil
}

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
