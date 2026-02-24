package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/kvi"
	"github.com/thirawat27/kvi/pkg/types"
)

func TestEnginePut(t *testing.T) {
	eng, err := kvi.Open(config.MemoryConfig())
	assert.NoError(t, err)
	defer eng.Close()

	ctx := context.Background()
	record := &types.Record{ID: "key1", Data: map[string]interface{}{"value": "test"}}

	err = eng.Put(ctx, "key1", record)
	assert.NoError(t, err)

	retrieved, err := eng.Get(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, "test", retrieved.Data["value"])
}
