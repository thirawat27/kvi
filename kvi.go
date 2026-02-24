// Package kvi provides a multi-modal embedded database
package kvi

import (
	"context"
	"time"

	"github.com/thirawat27/kvi/internal/engine"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

// Version is the current version of Kvi
const Version = "1.0.0"

// Kvi represents a Kvi database instance
type Kvi struct {
	engine *engine.KviEngine
	config *config.Config
}

// Open opens or creates a Kvi database with the given configuration
func Open(cfg *config.Config) (*Kvi, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	eng, err := engine.NewEngine(cfg)
	if err != nil {
		return nil, err
	}

	return &Kvi{
		engine: eng,
		config: cfg,
	}, nil
}

// OpenMemory opens an in-memory database
func OpenMemory() (*Kvi, error) {
	return Open(config.MemoryConfig())
}

// OpenDisk opens a persistent database
func OpenDisk(dataDir string) (*Kvi, error) {
	cfg := config.DiskConfig()
	cfg.DataDir = dataDir
	cfg.WALPath = dataDir + "/wal.log"
	return Open(cfg)
}

// OpenVector opens a vector-optimized database
func OpenVector(dimensions int) (*Kvi, error) {
	return Open(config.VectorConfig(dimensions))
}

// Get retrieves a record by key
func (k *Kvi) Get(ctx context.Context, key string) (*types.Record, error) {
	return k.engine.Get(ctx, key)
}

// Put stores a record
func (k *Kvi) Put(ctx context.Context, key string, value *types.Record) error {
	return k.engine.Put(ctx, key, value)
}

// Delete removes a record
func (k *Kvi) Delete(ctx context.Context, key string) error {
	return k.engine.Delete(ctx, key)
}

// Scan retrieves records in a key range
func (k *Kvi) Scan(ctx context.Context, start, end string, limit int) ([]*types.Record, error) {
	return k.engine.Scan(ctx, start, end, limit)
}

// BatchPut stores multiple records efficiently
func (k *Kvi) BatchPut(ctx context.Context, entries map[string]*types.Record) error {
	return k.engine.BatchPut(ctx, entries)
}

// VectorSearch performs similarity search on vectors
func (k *Kvi) VectorSearch(query []float32, topK int) ([]string, []float32, error) {
	return k.engine.VectorSearch(query, topK)
}

// GetAsOf retrieves a record as of a specific version (time-travel query)
func (k *Kvi) GetAsOf(key string, asOfTx uint64) (*types.Record, error) {
	return k.engine.GetAsOf(key, asOfTx)
}

// Snapshot creates a point-in-time snapshot
func (k *Kvi) Snapshot() (types.Snapshot, error) {
	return k.engine.Snapshot()
}

// Restore restores from a snapshot
func (k *Kvi) Restore(snap types.Snapshot) error {
	return k.engine.Restore(snap)
}

// Stats returns database statistics
func (k *Kvi) Stats() types.EngineStats {
	return k.engine.Stats()
}

// Close closes the database
func (k *Kvi) Close() error {
	return k.engine.Close()
}

// Set is a convenience method to store a simple key-value pair
func (k *Kvi) Set(ctx context.Context, key string, value interface{}) error {
	record := &types.Record{
		ID: key,
		Data: map[string]interface{}{
			"value": value,
		},
	}
	return k.Put(ctx, key, record)
}

// GetString is a convenience method to retrieve a string value
func (k *Kvi) GetString(ctx context.Context, key string) (string, error) {
	record, err := k.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if val, ok := record.Data["value"].(string); ok {
		return val, nil
	}
	return "", nil
}

// SetWithTTL stores a record with time-to-live
func (k *Kvi) SetWithTTL(ctx context.Context, key string, value interface{}, ttlSeconds int) error {
	ttl := time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	record := &types.Record{
		ID:  key,
		TTL: &ttl,
		Data: map[string]interface{}{
			"value": value,
		},
	}
	return k.Put(ctx, key, record)
}

// SetVector stores a record with a vector
func (k *Kvi) SetVector(ctx context.Context, key string, vector []float32, metadata map[string]interface{}) error {
	record := &types.Record{
		ID:     key,
		Vector: vector,
		Data:   metadata,
	}
	return k.Put(ctx, key, record)
}
