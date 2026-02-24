package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thirawat27/kvi/internal/sql"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/kvi"
	"github.com/thirawat27/kvi/pkg/types"
)

func TestSQLExecutor(t *testing.T) {
	eng, err := kvi.Open(config.MemoryConfig())
	assert.NoError(t, err)
	defer eng.Close()

	ctx := context.Background()
	executor := sql.NewExecutor(eng)

	// Test Standard SQL INSERT
	_, err = executor.ExecuteQuery(ctx, "INSERT INTO users (id, name, age) VALUES ('user1', 'John', 30)")
	assert.NoError(t, err)

	rec, err := eng.Get(ctx, "user1")
	assert.NoError(t, err)
	assert.Equal(t, "user1", rec.ID)
	assert.Equal(t, "John", rec.Data["name"])
	assert.Equal(t, int64(30), rec.Data["age"]) // integers are correctly typed as int64

	// Test Standard SQL SELECT
	result, err := executor.ExecuteQuery(ctx, "SELECT * FROM users WHERE id = 'user1'")
	assert.NoError(t, err)
	recRes := result.(*types.Record)
	assert.Equal(t, "user1", recRes.ID)
	assert.Equal(t, "John", recRes.Data["name"])

	// Test Standard SQL UPDATE
	_, err = executor.ExecuteQuery(ctx, "UPDATE users SET name = 'Jane', age = 31 WHERE id = 'user1'")
	assert.NoError(t, err)

	rec2, err := eng.Get(ctx, "user1")
	assert.NoError(t, err)
	assert.Equal(t, "Jane", rec2.Data["name"])
	assert.Equal(t, int64(31), rec2.Data["age"])

	// Test Standard SQL DELETE
	_, err = executor.ExecuteQuery(ctx, "DELETE FROM users WHERE id = 'user1'")
	assert.NoError(t, err)
	_, err = eng.Get(ctx, "user1")
	assert.Error(t, err) // Should error indicating it is not found
}
