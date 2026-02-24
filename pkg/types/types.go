package types

import "context"

type Mode string

const (
	ModeMemory   Mode = "memory"
	ModeDisk     Mode = "disk"
	ModeColumnar Mode = "columnar"
	ModeVector   Mode = "vector"
	ModeHybrid   Mode = "hybrid"
)

type Operation string

const (
	OpPut    Operation = "PUT"
	OpDelete Operation = "DELETE"
	OpBatch  Operation = "BATCH"
)

type ColumnType string

const (
	ColTypeInt    ColumnType = "int"
	ColTypeString ColumnType = "string"
	ColTypeFloat  ColumnType = "float"
	ColTypeBool   ColumnType = "bool"
)

type Record struct {
	ID   string                 `json:"id"`
	Data map[string]interface{} `json:"data"`
}

type Engine interface {
	Put(ctx context.Context, key string, record *Record) error
	Get(ctx context.Context, key string) (*Record, error)
	Delete(ctx context.Context, key string) error
	Close() error
}
