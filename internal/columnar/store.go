package columnar

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/thirawat27/kvi/pkg/types"
)

type ColumnStats struct {
	Min       float64
	Max       float64
	Count     int
	NullCount int
}

type Column struct {
	Name       string
	Type       types.ColumnType
	Data       []interface{}
	Compressed []byte
	Stats      *ColumnStats
}

type Block struct {
	ID      int
	Columns map[string]*Column
	Rows    int
}

type ColumnarStore struct {
	blocks      []*Block
	blockSize   int
	compression bool
	encoder     *zstd.Encoder
	decoder     *zstd.Decoder
}

func NewColumnarStore(blockSize int, compress bool) (*ColumnarStore, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	return &ColumnarStore{
		blocks:      make([]*Block, 0),
		blockSize:   blockSize,
		compression: compress,
		encoder:     enc,
		decoder:     dec,
	}, nil
}

func (s *ColumnarStore) Insert(records []*types.Record) error {
	if len(records) == 0 {
		return nil
	}

	// Create a new block or append to the last one if it has space
	var currentBlock *Block
	if len(s.blocks) > 0 {
		last := s.blocks[len(s.blocks)-1]
		if last.Rows < s.blockSize {
			currentBlock = last
		}
	}

	if currentBlock == nil {
		currentBlock = &Block{
			ID:      len(s.blocks),
			Columns: make(map[string]*Column),
			Rows:    0,
		}
		s.blocks = append(s.blocks, currentBlock)
	}

	for _, rec := range records {
		for colName, val := range rec.Data {
			col, exists := currentBlock.Columns[colName]
			if !exists {
				// infer type
				colType := inferType(val)
				col = &Column{
					Name:  colName,
					Type:  colType,
					Data:  make([]interface{}, 0),
					Stats: &ColumnStats{Min: math.MaxFloat64, Max: -math.MaxFloat64},
				}
				currentBlock.Columns[colName] = col
			}
			col.Data = append(col.Data, val)
			updateStats(col.Stats, val)
		}
		currentBlock.Rows++

		// If block is full, compress it
		if currentBlock.Rows >= s.blockSize {
			if s.compression {
				s.compressBlock(currentBlock)
			}
			currentBlock = &Block{
				ID:      len(s.blocks),
				Columns: make(map[string]*Column),
				Rows:    0,
			}
			s.blocks = append(s.blocks, currentBlock)
		}
	}

	return nil
}

func (s *ColumnarStore) compressBlock(block *Block) {
	for _, col := range block.Columns {
		if len(col.Data) == 0 {
			continue
		}
		// In a real system we would serialize the data array into bytes, compress it, and nil the Data array to save memory.
		// For simplicity, we just serialize via gob/json here - but we'll mock it for brevity.
		var buf bytes.Buffer
		for _, v := range col.Data {
			buf.WriteString(fmt.Sprintf("%v,", v))
		}
		compressed := s.encoder.EncodeAll(buf.Bytes(), make([]byte, 0, len(buf.Bytes())))
		col.Compressed = compressed

		// Unset uncompressed data to save memory
		// col.Data = nil
	}
}

func (s *ColumnarStore) DecompressBlock(block *Block) error {
	for _, col := range block.Columns {
		if len(col.Compressed) > 0 && len(col.Data) == 0 {
			decompressed, err := s.decoder.DecodeAll(col.Compressed, nil)
			if err != nil && err != io.ErrUnexpectedEOF { // Ignore EOF for simple string buffer
				return err
			}
			// normally we would deserialize here back to col.Data
			_ = decompressed
		}
	}
	return nil
}

func (s *ColumnarStore) Sum(columnName string) (float64, error) {
	var total float64
	found := false
	for _, block := range s.blocks {
		col, exists := block.Columns[columnName]
		if !exists {
			continue
		}
		found = true
		for _, val := range col.Data {
			if fval, ok := val.(float64); ok {
				total += fval
			} else if ival, ok := val.(int); ok {
				total += float64(ival)
			}
		}
	}
	if !found {
		return 0, errors.New("column not found")
	}
	return total, nil
}

func inferType(val interface{}) types.ColumnType {
	switch val.(type) {
	case int, int32, int64:
		return types.ColTypeInt
	case float32, float64:
		return types.ColTypeFloat
	case bool:
		return types.ColTypeBool
	default:
		return types.ColTypeString
	}
}

func updateStats(stats *ColumnStats, val interface{}) {
	var fval float64
	switch v := val.(type) {
	case int:
		fval = float64(v)
	case float64:
		fval = v
	default:
		return // non-numeric
	}

	if fval < stats.Min {
		stats.Min = fval
	}
	if fval > stats.Max {
		stats.Max = fval
	}
	stats.Count++
}
