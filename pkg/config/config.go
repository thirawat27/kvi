package config

import (
	"time"
	"github.com/thirawat27/kvi/pkg/types"
)

// Config ครอบคลุมทุก use case
type Config struct {
	// Engine Mode
	Mode types.Mode `json:"mode"`

	// Paths
	DataDir     string `json:"data_dir"` // สำหรับ Disk/Hybrid mode
	WALPath     string `json:"wal_path"`
	SnapshotDir string `json:"snapshot_dir"`

	// Memory Settings
	MaxMemoryMB    int `json:"max_memory_mb"` // Soft limit
	CacheSizeMB    int `json:"cache_size_mb"`
	MemtableSizeMB int `json:"memtable_size_mb"` // Flush threshold

	// Performance
	MaxConnections int           `json:"max_connections"`
	QueryTimeout   time.Duration `json:"query_timeout"`
	SyncInterval   time.Duration `json:"sync_interval"` // 0 = sync every write

	// Columnar
	ColumnarBlockSize int `json:"columnar_block_size"` // Default 10000 rows

	// Vector
	VectorDimensions int `json:"vector_dimensions"` // 384, 768, 1536
	HNSWM            int `json:"hnsw_m"`            // Default 16
	HNSWEf           int `json:"hnsw_ef"`           // Default 200

	// Features
	EnableWAL      bool `json:"enable_wal"`
	EnablePubSub   bool `json:"enable_pubsub"`
	EnableChecksum bool `json:"enable_checksum"`
	Compression    bool `json:"compression"` // ZSTD

	// Security
	EncryptionKey string `json:"encryption_key"` // AES-256 (optional)

	// Network
	Host string `json:"host"`
	Port int    `json:"port"`
}

// DefaultConfig สำหรับเริ่มต้นใช้งานทันที
func DefaultConfig() *Config {
	return &Config{
		Mode:              types.ModeHybrid,
		MaxMemoryMB:       1024,
		CacheSizeMB:       256,
		MaxConnections:    100,
		SyncInterval:      0, // Full durability
		ColumnarBlockSize: 10000,
		VectorDimensions:  384,
		HNSWM:             16,
		HNSWEf:            200,
		EnableWAL:         true,
		EnablePubSub:      true,
		EnableChecksum:    true,
		Compression:       true,
		Host:              "localhost",
		Port:              8080,
	}
}

// MemoryConfig สำหรับ in-memory mode (เร็วที่สุด)
func MemoryConfig() *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeMemory
	cfg.EnableWAL = false
	cfg.MaxMemoryMB = 512
	return cfg
}

// DiskConfig สำหรับ persistent mode (ปลอดภัยที่สุด)
func DiskConfig() *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeDisk
	cfg.EnableWAL = true
	cfg.DataDir = "./data"
	cfg.WALPath = "./data/wal.log"
	cfg.SnapshotDir = "./data/snapshots"
	return cfg
}

// ColumnarConfig สำหรับ analytics mode
func ColumnarConfig() *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeColumnar
	cfg.ColumnarBlockSize = 50000
	cfg.Compression = true
	return cfg
}

// VectorConfig สำหรับ AI/ML mode
func VectorConfig(dimensions int) *Config {
	cfg := DefaultConfig()
	cfg.Mode = types.ModeVector
	cfg.VectorDimensions = dimensions
	cfg.HNSWM = 32 // Higher M for better recall
	cfg.HNSWEf = 400
	return cfg
}

// Validate ตรวจสอบความถูกต้องของ config
func (c *Config) Validate() error {
	if c.MaxMemoryMB <= 0 {
		c.MaxMemoryMB = 1024
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 100
	}
	if c.VectorDimensions <= 0 {
		c.VectorDimensions = 384
	}
	if c.HNSWM <= 0 {
		c.HNSWM = 16
	}
	if c.HNSWEf <= 0 {
		c.HNSWEf = 200
	}
	if c.ColumnarBlockSize <= 0 {
		c.ColumnarBlockSize = 10000
	}
	return nil
}
