package tests

import (
	"context"
	"testing"
	"time"

	"github.com/thirawat27/kvi"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

func TestMemoryEngine_BasicCRUD(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test Put
	record := &types.Record{
		ID: "test1",
		Data: map[string]interface{}{
			"name":  "test",
			"value": 123,
		},
	}

	if err := db.Put(ctx, "test1", record); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Test Get
	got, err := db.Get(ctx, "test1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if got.ID != "test1" {
		t.Errorf("Expected ID test1, got %s", got.ID)
	}

	if got.Data["name"] != "test" {
		t.Errorf("Expected name test, got %v", got.Data["name"])
	}

	// Test Delete
	if err := db.Delete(ctx, "test1"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify deletion
	_, err = db.Get(ctx, "test1")
	if err == nil {
		t.Error("Expected error after delete, got nil")
	}
}

func TestMemoryEngine_Scan(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Insert multiple records
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		record := &types.Record{
			ID: key,
			Data: map[string]interface{}{
				"index": i,
			},
		}
		if err := db.Put(ctx, key, record); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Test Scan
	records, err := db.Scan(ctx, "a", "f", 5)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	if len(records) != 5 {
		t.Errorf("Expected 5 records, got %d", len(records))
	}
}

func TestMemoryEngine_BatchPut(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	entries := make(map[string]*types.Record)
	for i := 0; i < 100; i++ {
		key := string(rune(i))
		entries[key] = &types.Record{
			ID: key,
			Data: map[string]interface{}{
				"value": i,
			},
		}
	}

	if err := db.BatchPut(ctx, entries); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	// Verify
	for i := 0; i < 100; i++ {
		key := string(rune(i))
		_, err := db.Get(ctx, key)
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
		}
	}
}

func TestMemoryEngine_TTL(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Set with TTL
	if err := db.SetWithTTL(ctx, "ttl_key", "value", 1); err != nil {
		t.Fatalf("Failed to set with TTL: %v", err)
	}

	// Should exist immediately
	_, err = db.Get(ctx, "ttl_key")
	if err != nil {
		t.Fatalf("Expected key to exist: %v", err)
	}

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Should be expired
	_, err = db.Get(ctx, "ttl_key")
	if err == nil {
		t.Error("Expected key to be expired")
	}
}

func TestMemoryEngine_SnapshotRestore(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Insert data
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		record := &types.Record{
			ID: key,
			Data: map[string]interface{}{
				"index": i,
			},
		}
		if err := db.Put(ctx, key, record); err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	// Create snapshot
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Clear data
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		db.Delete(ctx, key)
	}

	// Restore
	if err := db.Restore(snap); err != nil {
		t.Fatalf("Failed to restore: %v", err)
	}

	// Verify
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		_, err := db.Get(ctx, key)
		if err != nil {
			t.Errorf("Failed to get %s after restore: %v", key, err)
		}
	}
}

func TestMemoryEngine_Stats(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Insert some data
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		record := &types.Record{ID: key}
		if err := db.Put(ctx, key, record); err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	stats := db.Stats()

	if stats.RecordsTotal != 10 {
		t.Errorf("Expected 10 records, got %d", stats.RecordsTotal)
	}
}

func TestVectorSearch(t *testing.T) {
	cfg := config.VectorConfig(3) // 3 dimensions for testing
	db, err := kvi.Open(cfg)
	if err != nil {
		t.Fatalf("Failed to open vector engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Insert vectors
	vectors := map[string][]float32{
		"vec1": {1.0, 0.0, 0.0},
		"vec2": {0.9, 0.1, 0.0},
		"vec3": {0.0, 1.0, 0.0},
		"vec4": {0.0, 0.0, 1.0},
	}

	for key, vec := range vectors {
		if err := db.SetVector(ctx, key, vec, nil); err != nil {
			t.Fatalf("Failed to set vector %s: %v", key, err)
		}
	}

	// Search for similar to vec1
	query := []float32{0.95, 0.05, 0.0}
	ids, scores, err := db.VectorSearch(query, 2)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}

	if len(ids) != 2 {
		t.Errorf("Expected 2 results, got %d", len(ids))
	}

	// First result should be vec1 or vec2 (most similar)
	if ids[0] != "vec1" && ids[0] != "vec2" {
		t.Errorf("Expected vec1 or vec2 as first result, got %s", ids[0])
	}

	if scores[0] < 0.9 {
		t.Errorf("Expected high similarity score, got %f", scores[0])
	}
}

func TestConvenienceMethods(t *testing.T) {
	db, err := kvi.OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory engine: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test Set
	if err := db.Set(ctx, "key1", "value1"); err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	// Test GetString
	val, err := db.GetString(ctx, "key1")
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}

	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

func TestConfigModes(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
	}{
		{"memory", config.MemoryConfig()},
		{"columnar", config.ColumnarConfig()},
		{"vector", config.VectorConfig(128)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := kvi.Open(tt.config)
			if err != nil {
				t.Fatalf("Failed to open %s engine: %v", tt.name, err)
			}
			defer db.Close()

			ctx := context.Background()
			record := &types.Record{ID: "test"}
			if err := db.Put(ctx, "test", record); err != nil {
				t.Fatalf("Failed to put: %v", err)
			}
		})
	}
}

// Benchmark tests
func BenchmarkPut(b *testing.B) {
	db, _ := kvi.OpenMemory()
	defer db.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune(i))
		record := &types.Record{
			ID:   key,
			Data: map[string]interface{}{"value": i},
		}
		db.Put(ctx, key, record)
	}
}

func BenchmarkGet(b *testing.B) {
	db, _ := kvi.OpenMemory()
	defer db.Close()

	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := string(rune(i))
		record := &types.Record{ID: key}
		db.Put(ctx, key, record)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune(i % 10000))
		db.Get(ctx, key)
	}
}

func BenchmarkVectorSearch(b *testing.B) {
	cfg := config.VectorConfig(128)
	db, _ := kvi.Open(cfg)
	defer db.Close()

	ctx := context.Background()

	// Pre-populate with vectors
	for i := 0; i < 1000; i++ {
		key := string(rune(i))
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i+j) / 1000.0
		}
		db.SetVector(ctx, key, vec, nil)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.VectorSearch(query, 10)
	}
}
