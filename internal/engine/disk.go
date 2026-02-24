package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/thirawat27/kvi/internal/wal"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

type btreeItem struct {
	key string
	rec *types.Record
}

func (i btreeItem) Less(than btree.Item) bool {
	return i.key < than.(btreeItem).key
}

type DiskEngine struct {
	config *config.Config
	tree   *btree.BTree
	wal    *wal.WAL
	mu     sync.RWMutex
}

func NewDiskEngine(cfg *config.Config) (*DiskEngine, error) {
	walDB, err := wal.NewWAL(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	// In real DB, we would recover from WAL here.
	// We'll skip WAL recovery implementation for simplicity of stub.

	return &DiskEngine{
		config: cfg,
		tree:   btree.New(32), // degree 32
		wal:    walDB,
	}, nil
}

func (e *DiskEngine) Put(ctx context.Context, key string, record *types.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.config.EnableWAL {
		if err := e.wal.WriteEntry(types.OpPut, key, record); err != nil {
			return err
		}
	}

	e.tree.ReplaceOrInsert(btreeItem{key: key, rec: record})
	return nil
}

func (e *DiskEngine) Get(ctx context.Context, key string) (*types.Record, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	item := e.tree.Get(btreeItem{key: key})
	if item == nil {
		return nil, fmt.Errorf("record not found for key: %s", key)
	}
	return item.(btreeItem).rec, nil
}

func (e *DiskEngine) Delete(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.config.EnableWAL {
		if err := e.wal.WriteEntry(types.OpDelete, key, nil); err != nil {
			return err
		}
	}

	e.tree.Delete(btreeItem{key: key})
	return nil
}

func (e *DiskEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.config.EnableWAL {
		return e.wal.Close()
	}
	return nil
}

// Compile time check
var _ types.Engine = (*DiskEngine)(nil)
