package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

type MemoryEngine struct {
	config  *config.Config
	records map[string]*types.Record
	mu      sync.RWMutex
}

func NewMemoryEngine(cfg *config.Config) *MemoryEngine {
	return &MemoryEngine{
		config:  cfg,
		records: make(map[string]*types.Record),
	}
}

func (e *MemoryEngine) Put(ctx context.Context, key string, record *types.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.records[key] = record
	return nil
}

func (e *MemoryEngine) Get(ctx context.Context, key string) (*types.Record, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if record, exists := e.records[key]; exists {
		return record, nil
	}
	return nil, fmt.Errorf("record not found for key: %s", key)
}

func (e *MemoryEngine) Delete(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.records, key)
	return nil
}

func (e *MemoryEngine) Close() error {
	return nil
}

// Compile time check
var _ types.Engine = (*MemoryEngine)(nil)
