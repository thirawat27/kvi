package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thirawat27/kvi/internal/columnar"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

type ColumnarEngine struct {
	config  *config.Config
	records map[string]*types.Record
	store   *columnar.ColumnarStore
	mu      sync.RWMutex
}

func NewColumnarEngine(cfg *config.Config) (*ColumnarEngine, error) {
	store, err := columnar.NewColumnarStore(10000, true) // compress after 10,000 rows
	if err != nil {
		return nil, err
	}

	return &ColumnarEngine{
		config:  cfg,
		records: make(map[string]*types.Record),
		store:   store,
	}, nil
}

func (e *ColumnarEngine) Put(ctx context.Context, key string, record *types.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.records[key] = record
	err := e.store.Insert([]*types.Record{record})
	if err != nil {
		return fmt.Errorf("columnar insert failed: %v", err)
	}

	return nil
}

func (e *ColumnarEngine) Get(ctx context.Context, key string) (*types.Record, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	record, ok := e.records[key]
	if !ok {
		return nil, fmt.Errorf("record not found for key: %s", key)
	}
	return record, nil
}

func (e *ColumnarEngine) Delete(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Columnar stores are append-only. Deletes are usually handled via tombstone bitmaps
	// Since this is simplified, we'll just delete the map reference
	delete(e.records, key)
	return nil
}

func (e *ColumnarEngine) Close() error {
	return nil
}

func (e *ColumnarEngine) Sum(columnName string) (float64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Mock analytics delay
	time.Sleep(5 * time.Millisecond)

	return e.store.Sum(columnName)
}

var _ types.Engine = (*ColumnarEngine)(nil)
