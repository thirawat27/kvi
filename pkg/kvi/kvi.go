package kvi

import (
	"github.com/thirawat27/kvi/internal/engine"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

// Open opens an engine with the given configuration
func Open(cfg *config.Config) (types.Engine, error) {
	return engine.NewEngine(cfg)
}

// OpenMemory creates a pure in-memory engine
func OpenMemory() (types.Engine, error) {
	return Open(config.MemoryConfig())
}

// OpenDisk creates a persistent disk-backed engine
func OpenDisk(dataDir string) (types.Engine, error) {
	cfg := config.DiskConfig()
	cfg.DataDir = dataDir
	return Open(cfg)
}

// OpenVector creates a vector search engine
func OpenVector(dim int) (types.Engine, error) {
	return Open(config.VectorConfig(dim))
}
