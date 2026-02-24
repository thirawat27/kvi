package config

import "github.com/thirawat27/kvi/pkg/types"

type Config struct {
	Mode          types.Mode `json:"mode"`
	DataDir       string     `json:"data_dir"`
	MaxMemoryMB   int        `json:"max_memory_mb"`
	CacheSizeMB   int        `json:"cache_size_mb"`
	MemtableSpace int        `json:"memtable_size_mb"`
	EnableWAL     bool       `json:"enable_wal"`
	EnablePubSub  bool       `json:"enable_pubsub"`
	Port          int        `json:"port"`
	GrpcPort      int        `json:"grpc_port"`
	VectorDim     int        `json:"vector_dim"`
}

func DefaultConfig() *Config {
	return &Config{
		Mode:          types.ModeHybrid,
		DataDir:       "./data",
		MaxMemoryMB:   2048,
		CacheSizeMB:   256,
		MemtableSpace: 64,
		EnableWAL:     true,
		EnablePubSub:  true,
		Port:          8080,
		GrpcPort:      50051,
		VectorDim:     384,
	}
}

func MemoryConfig() *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeMemory
	cfg.EnableWAL = false
	return cfg
}

func DiskConfig() *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeDisk
	cfg.EnableWAL = true
	return cfg
}

func ColumnarConfig() *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeColumnar
	return cfg
}

func VectorConfig(dim int) *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeVector
	cfg.VectorDim = dim
	return cfg
}
