package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thirawat27/kvi/internal/vector"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

type VectorEngine struct {
	config  *config.Config
	records map[string]*types.Record
	index   *vector.HNSWIndex
	mu      sync.RWMutex
}

func NewVectorEngine(cfg *config.Config) (*VectorEngine, error) {
	if cfg.VectorDim <= 0 {
		return nil, fmt.Errorf("vector dim must be > 0")
	}

	return &VectorEngine{
		config:  cfg,
		records: make(map[string]*types.Record),
		index:   vector.NewHNSWIndex(cfg.VectorDim),
	}, nil
}

func (e *VectorEngine) Put(ctx context.Context, key string, record *types.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Need a vector field from Record, here we extract it, assume "vector" key in Data map holds []float32
	vecVal, ok := record.Data["vector"]
	if !ok {
		return fmt.Errorf("record missing 'vector' key")
	}

	vec, ok := vecVal.([]float32)
	if !ok {
		return fmt.Errorf("vector must be []float32")
	}

	e.records[key] = record
	e.index.Add(key, vec)
	return nil
}

func (e *VectorEngine) Get(ctx context.Context, key string) (*types.Record, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	record, ok := e.records[key]
	if !ok {
		return nil, fmt.Errorf("record not found for key: %s", key)
	}
	return record, nil
}

func (e *VectorEngine) Delete(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.records, key)
	e.index.Delete(key)
	return nil
}

func (e *VectorEngine) Close() error {
	return nil
}

func (e *VectorEngine) Search(ctx context.Context, query []float32, k int) ([]*types.Record, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// mock search delay
	time.Sleep(10 * time.Millisecond)

	ids := e.index.Search(query, k)
	var results []*types.Record
	for _, id := range ids {
		if rec, exists := e.records[id]; exists {
			results = append(results, rec)
		}
	}
	return results, nil
}

var _ types.Engine = (*VectorEngine)(nil)
