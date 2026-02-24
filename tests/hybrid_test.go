package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/kvi"
	"github.com/thirawat27/kvi/pkg/types"
)

func TestHybridEnginePutGetDelete(t *testing.T) {
	// Setup test directory
	testDir := "./test_hybrid_data"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	cfg := config.DefaultConfig()
	cfg.Mode = types.ModeHybrid
	cfg.DataDir = testDir

	eng, err := kvi.Open(cfg)
	assert.NoError(t, err)

	ctx := context.Background()
	rec := &types.Record{
		ID:   "hkey1",
		Data: map[string]interface{}{"val": 100},
	}

	err = eng.Put(ctx, "hkey1", rec)
	assert.NoError(t, err)

	// Wait for async flush to finish
	time.Sleep(150 * time.Millisecond)

	retrieved, err := eng.Get(ctx, "hkey1")
	assert.NoError(t, err)
	assert.Equal(t, 100, retrieved.Data["val"])

	// Test Deletion
	err = eng.Delete(ctx, "hkey1")
	assert.NoError(t, err)

	_, err = eng.Get(ctx, "hkey1")
	assert.Error(t, err)

	err = eng.Close()
	assert.NoError(t, err)
}
