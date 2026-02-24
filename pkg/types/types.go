package types

import (
	"context"
	"time"
)

// Engine เป็น interface หลักที่ทุก mode ต้อง implement
type Engine interface {
	// Basic CRUD
	Get(ctx context.Context, key string) (*Record, error)
	Put(ctx context.Context, key string, value *Record) error
	Delete(ctx context.Context, key string) error
	Scan(ctx context.Context, start, end string, limit int) ([]*Record, error)

	// Advanced
	BatchPut(ctx context.Context, entries map[string]*Record) error
	Snapshot() (Snapshot, error)
	Restore(snap Snapshot) error
	Close() error

	// Stats
	Stats() EngineStats
}

// Record คือหน่วยข้อมูลพื้นฐานที่รองรับทุกรูปแบบ
type Record struct {
	ID       string                 `json:"id"`
	Data     map[string]interface{} `json:"data"`      // Schema-less JSONB
	Vector   []float32              `json:"vector"`    // For AI search
	Version  uint64                 `json:"version"`   // MVCC timestamp
	TTL      *time.Time             `json:"ttl"`       // Expiration (Redis-style)
	Checksum uint32                 `json:"checksum"`  // CRC32 integrity

	// Metadata
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// EngineStats สำหรับ monitoring
type EngineStats struct {
	RecordsTotal  int64
	MemoryUsed    int64
	DiskUsed      int64
	CacheHitRatio float64
	AvgQueryTime  time.Duration
	WALSize       int64
}

// Snapshot สำหรับ backup/restore
type Snapshot struct {
	Version  uint64
	Records  map[string]*Record
	CreatedAt time.Time
	Checksum uint32
}

// Mode กำหนดพฤติกรรมของ engine
type Mode int

const (
	ModeMemory Mode = iota    // Pure in-memory (O(1) hashmap)
	ModeDisk                  // Persistent with WAL (SQLite-style)
	ModeColumnar              // Analytical (DuckDB-style)
	ModeVector                // AI-optimized (Pinecone-style)
	ModeHybrid                // Auto-switch based on workload
)

func (m Mode) String() string {
	switch m {
	case ModeMemory:
		return "memory"
	case ModeDisk:
		return "disk"
	case ModeColumnar:
		return "columnar"
	case ModeVector:
		return "vector"
	case ModeHybrid:
		return "hybrid"
	default:
		return "unknown"
	}
}

// Operation สำหรับ WAL
type Operation byte

const (
	OpPut Operation = iota
	OpDelete
	OpBatch
	OpCheckpoint
)

func (o Operation) String() string {
	switch o {
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	case OpBatch:
		return "BATCH"
	case OpCheckpoint:
		return "CHECKPOINT"
	default:
		return "UNKNOWN"
	}
}

// ColumnType สำหรับ columnar storage
type ColumnType int

const (
	TypeInt ColumnType = iota
	TypeFloat
	TypeString
	TypeJSON
	TypeVector
)

func (c ColumnType) String() string {
	switch c {
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeString:
		return "string"
	case TypeJSON:
		return "json"
	case TypeVector:
		return "vector"
	default:
		return "unknown"
	}
}

// Errors
type KviError struct {
	Code    int
	Message string
}

func (e *KviError) Error() string {
	return e.Message
}

var (
	ErrKeyNotFound      = &KviError{Code: 1, Message: "key not found"}
	ErrDataCorruption   = &KviError{Code: 2, Message: "data corruption detected"}
	ErrInvalidMode      = &KviError{Code: 3, Message: "invalid engine mode"}
	ErrInvalidVector    = &KviError{Code: 4, Message: "invalid vector dimension"}
	ErrChannelNotFound  = &KviError{Code: 5, Message: "channel not found"}
	ErrWALRecovery      = &KviError{Code: 6, Message: "wal recovery failed"}
	ErrSnapshotFailed   = &KviError{Code: 7, Message: "snapshot failed"}
	ErrRestoreFailed    = &KviError{Code: 8, Message: "restore failed"}
	ErrInvalidQuery     = &KviError{Code: 9, Message: "invalid query"}
	ErrTimeout          = &KviError{Code: 10, Message: "operation timeout"}
	ErrMemoryLimit      = &KviError{Code: 11, Message: "memory limit exceeded"}
	ErrConnectionLimit  = &KviError{Code: 12, Message: "connection limit exceeded"}
)