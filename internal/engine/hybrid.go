package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

type HybridEngine struct {
	config      *config.Config
	memory      *MemoryEngine
	disk        *DiskEngine
	vectorStore *VectorEngine
	columnStore *ColumnarEngine

	mu        sync.RWMutex
	writeChan chan *types.Record
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewHybridEngine(cfg *config.Config) (*HybridEngine, error) {
	mem := NewMemoryEngine(cfg)

	disk, err := NewDiskEngine(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to init disk engine: %w", err)
	}

	vecConfig := config.VectorConfig(cfg.VectorDim)
	vec, err := NewVectorEngine(vecConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to init vector engine: %w", err)
	}

	col, err := NewColumnarEngine(config.ColumnarConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to init columnar engine: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	h := &HybridEngine{
		config:      cfg,
		memory:      mem,
		disk:        disk,
		vectorStore: vec,
		columnStore: col,
		writeChan:   make(chan *types.Record, 1000),
		ctx:         ctx,
		cancel:      cancel,
	}

	h.wg.Add(1)
	go h.asyncWorker()

	return h, nil
}

func (h *HybridEngine) asyncWorker() {
	defer h.wg.Done()

	for {
		select {
		case <-h.ctx.Done():
			// Flush remaining
			for len(h.writeChan) > 0 {
				rec := <-h.writeChan
				_ = h.disk.Put(context.Background(), rec.ID, rec)
				_ = h.columnStore.Put(context.Background(), rec.ID, rec)
			}
			return
		case rec := <-h.writeChan:
			// Write to disk
			if err := h.disk.Put(context.Background(), rec.ID, rec); err != nil {
				fmt.Printf("Disk async write error: %v\n", err)
			}
			// Write to columnar
			if err := h.columnStore.Put(context.Background(), rec.ID, rec); err != nil {
				fmt.Printf("Columnar async write error: %v\n", err)
			}
		}
	}
}

func (h *HybridEngine) Put(ctx context.Context, key string, record *types.Record) error {
	// 1. Sync write to Memory for fast access
	if err := h.memory.Put(ctx, key, record); err != nil {
		return err
	}

	// 2. Check if vector data exists
	if _, ok := record.Data["vector"]; ok {
		if err := h.vectorStore.Put(ctx, key, record); err != nil {
			return err
		}
	}

	// 3. Async write to disk & columnar
	select {
	case h.writeChan <- record:
	case <-time.After(100 * time.Millisecond):
		return fmt.Errorf("async write queue full")
	}

	return nil
}

func (h *HybridEngine) Get(ctx context.Context, key string) (*types.Record, error) {
	// First check memory
	if rec, err := h.memory.Get(ctx, key); err == nil {
		return rec, nil
	}

	// Fallback to disk
	rec, err := h.disk.Get(ctx, key)
	if err == nil {
		// Populate memory
		_ = h.memory.Put(ctx, key, rec)
		return rec, nil
	}

	return nil, err
}

func (h *HybridEngine) Delete(ctx context.Context, key string) error {
	// Delete from memory and disk synchronously to ensure data integrity
	_ = h.memory.Delete(ctx, key)
	_ = h.vectorStore.Delete(ctx, key)
	_ = h.columnStore.Delete(ctx, key)
	return h.disk.Delete(ctx, key)
}

func (h *HybridEngine) Close() error {
	h.cancel()
	h.wg.Wait()

	h.memory.Close()
	h.vectorStore.Close()
	h.columnStore.Close()
	return h.disk.Close()
}

func (h *HybridEngine) Search(ctx context.Context, query []float32, k int) ([]*types.Record, error) {
	return h.vectorStore.Search(ctx, query, k)
}

func (h *HybridEngine) Sum(columnName string) (float64, error) {
	return h.columnStore.Sum(columnName)
}

var _ types.Engine = (*HybridEngine)(nil)
