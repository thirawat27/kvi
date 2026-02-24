package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/thirawat27/kvi/pkg/types"
)

// ColumnarStore stores data in column-oriented format for analytics
type ColumnarStore struct {
	columns     map[string]*Column
	blocks      []*Block
	blockSize   int
	compression bool
	mu          sync.RWMutex
	encoder     *zstd.Encoder
	decoder     *zstd.Decoder
	encoderBuf  *bytes.Buffer
}

// Column represents a single column with statistics
type Column struct {
	Name       string           `json:"name"`
	Type       types.ColumnType `json:"type"`
	Data       []interface{}    `json:"data"`       // Chunked storage
	Compressed []byte           `json:"compressed"` // ZSTD compressed
	Min        interface{}      `json:"min"`        // Statistics for pruning
	Max        interface{}      `json:"max"`
	NullCount  int              `json:"null_count"`
	RowCount   int              `json:"row_count"`
}

// Block represents a block of columnar data
type Block struct {
	ID        uint32    `json:"id"`
	MinKey    string    `json:"min_key"`
	MaxKey    string    `json:"max_key"`
	RowCount  int       `json:"row_count"`
	Checksum  uint32    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
}

// AggQuery represents an aggregation query
type AggQuery struct {
	Column    string
	Operation string // SUM, AVG, COUNT, MIN, MAX
	Filter    *Filter
}

// Filter represents a filter for queries
type Filter struct {
	Column   string
	Operator string // =, !=, >, <, >=, <=
	Value    interface{}
}

// Result represents a query result
type Result struct {
	Value       interface{}
	Count       int64
	Duration    time.Duration
	ScannedRows int64
}

// NewColumnarStore creates a new columnar store
func NewColumnarStore(blockSize int, compression bool) *ColumnarStore {
	cs := &ColumnarStore{
		columns:     make(map[string]*Column),
		blocks:      make([]*Block, 0),
		blockSize:   blockSize,
		compression: compression,
		encoderBuf:  bytes.NewBuffer(nil),
	}

	// Initialize zstd encoder/decoder
	if compression {
		enc, _ := zstd.NewWriter(cs.encoderBuf, zstd.WithEncoderLevel(zstd.SpeedDefault))
		cs.encoder = enc

		dec, _ := zstd.NewReader(nil)
		cs.decoder = dec
	}

	return cs
}

// InsertBatch inserts multiple records in batch mode
func (c *ColumnarStore) InsertBatch(records []*types.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Extract columns from records
	for _, rec := range records {
		// ID column
		if col, exists := c.columns["id"]; exists {
			col.Data = append(col.Data, rec.ID)
			col.RowCount++
		} else {
			c.columns["id"] = &Column{
				Name:     "id",
				Type:     types.TypeString,
				Data:     []interface{}{rec.ID},
				RowCount: 1,
			}
		}

		// Version column
		if col, exists := c.columns["version"]; exists {
			col.Data = append(col.Data, rec.Version)
			col.RowCount++
		} else {
			c.columns["version"] = &Column{
				Name:     "version",
				Type:     types.TypeInt,
				Data:     []interface{}{rec.Version},
				RowCount: 1,
			}
		}

		// Data fields
		for key, val := range rec.Data {
			if col, exists := c.columns[key]; exists {
				col.Data = append(col.Data, val)
				col.RowCount++
			} else {
				c.columns[key] = &Column{
					Name:     key,
					Type:     inferColumnType(val),
					Data:     []interface{}{val},
					RowCount: 1,
				}
			}
		}

		// Vector column if present
		if len(rec.Vector) > 0 {
			if col, exists := c.columns["vector"]; exists {
				col.Data = append(col.Data, rec.Vector)
				col.RowCount++
			} else {
				c.columns["vector"] = &Column{
					Name:     "vector",
					Type:     types.TypeVector,
					Data:     []interface{}{rec.Vector},
					RowCount: 1,
				}
			}
		}
	}

	// Compress blocks when threshold reached
	if c.columns["id"] != nil && len(c.columns["id"].Data) >= c.blockSize {
		return c.compressBlock()
	}

	return nil
}

// compressBlock compresses current data into a block
func (c *ColumnarStore) compressBlock() error {
	block := &Block{
		ID:        uint32(len(c.blocks)),
		RowCount:  len(c.columns["id"].Data),
		CreatedAt: time.Now(),
	}

	// Find min/max keys
	if idCol, exists := c.columns["id"]; exists && len(idCol.Data) > 0 {
		if min, ok := idCol.Data[0].(string); ok {
			block.MinKey = min
		}
		if max, ok := idCol.Data[len(idCol.Data)-1].(string); ok {
			block.MaxKey = max
		}
	}

	// Compress each column
	for name, col := range c.columns {
		if c.compression && len(col.Data) > 0 {
			compressed, err := c.compressColumn(col)
			if err != nil {
				return fmt.Errorf("failed to compress column %s: %w", name, err)
			}
			col.Compressed = compressed
			col.Data = nil // Free memory
		}

		// Update statistics
		c.updateColumnStats(col)
	}

	c.blocks = append(c.blocks, block)

	return nil
}

// compressColumn compresses column data using ZSTD
func (c *ColumnarStore) compressColumn(col *Column) ([]byte, error) {
	if c.encoder == nil {
		return nil, fmt.Errorf("encoder not initialized")
	}

	buf := bytes.NewBuffer(nil)
	enc, _ := zstd.NewWriter(buf, zstd.WithEncoderLevel(zstd.SpeedDefault))

	// Write column type
	if err := binary.Write(enc, binary.LittleEndian, uint8(col.Type)); err != nil {
		return nil, err
	}

	// Write row count
	if err := binary.Write(enc, binary.LittleEndian, uint32(len(col.Data))); err != nil {
		return nil, err
	}

	// Write data based on type
	for _, val := range col.Data {
		if err := encodeValue(enc, val, col.Type); err != nil {
			return nil, err
		}
	}

	if err := enc.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompressColumn decompresses column data
func (c *ColumnarStore) decompressColumn(col *Column) ([]interface{}, error) {
	if len(col.Compressed) == 0 {
		return col.Data, nil
	}

	dec, err := zstd.NewReader(bytes.NewReader(col.Compressed))
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	// Read column type
	var colType uint8
	if err := binary.Read(dec, binary.LittleEndian, &colType); err != nil {
		return nil, err
	}

	// Read row count
	var rowCount uint32
	if err := binary.Read(dec, binary.LittleEndian, &rowCount); err != nil {
		return nil, err
	}

	// Read data based on type
	data := make([]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		val, err := decodeValue(dec, types.ColumnType(colType))
		if err != nil {
			return nil, err
		}
		data[i] = val
	}

	return data, nil
}

// updateColumnStats updates min/max statistics for a column
func (c *ColumnarStore) updateColumnStats(col *Column) {
	if len(col.Data) == 0 {
		return
	}

	col.Min = col.Data[0]
	col.Max = col.Data[0]

	for _, val := range col.Data {
		if val == nil {
			col.NullCount++
			continue
		}

		if compareValues(val, col.Min) < 0 {
			col.Min = val
		}
		if compareValues(val, col.Max) > 0 {
			col.Max = val
		}
	}
}

// Aggregate performs aggregation on a column
func (c *ColumnarStore) Aggregate(query AggQuery) (Result, error) {
	start := time.Now()

	c.mu.RLock()
	defer c.mu.RUnlock()

	col, exists := c.columns[query.Column]
	if !exists {
		return Result{}, fmt.Errorf("column %s not found", query.Column)
	}

	// Decompress if needed
	data := col.Data
	if len(col.Compressed) > 0 {
		var err error
		data, err = c.decompressColumn(col)
		if err != nil {
			return Result{}, err
		}
	}

	var result interface{}
	var count int64

	switch query.Operation {
	case "COUNT":
		count = int64(len(data))
		result = count
	case "SUM":
		result = sumValues(data)
		count = int64(len(data))
	case "AVG":
		sum := sumValues(data)
		if len(data) > 0 {
			switch s := sum.(type) {
			case int:
				result = float64(s) / float64(len(data))
			case float64:
				result = s / float64(len(data))
			}
		}
		count = int64(len(data))
	case "MIN":
		result = col.Min
		count = int64(len(data))
	case "MAX":
		result = col.Max
		count = int64(len(data))
	default:
		return Result{}, fmt.Errorf("unsupported operation: %s", query.Operation)
	}

	return Result{
		Value:       result,
		Count:       count,
		Duration:    time.Since(start),
		ScannedRows: int64(len(data)),
	}, nil
}

// GetColumn retrieves a column by name
func (c *ColumnarStore) GetColumn(name string) (*Column, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	col, exists := c.columns[name]
	if !exists {
		return nil, fmt.Errorf("column %s not found", name)
	}

	return col, nil
}

// GetColumnNames returns all column names
func (c *ColumnarStore) GetColumnNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.columns))
	for name := range c.columns {
		names = append(names, name)
	}
	return names
}

// BlockCount returns the number of blocks
func (c *ColumnarStore) BlockCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.blocks)
}

// RowCount returns the total number of rows
func (c *ColumnarStore) RowCount() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if col, exists := c.columns["id"]; exists {
		return int64(col.RowCount)
	}
	return 0
}

// Stats returns columnar store statistics
func (c *ColumnarStore) Stats() ColumnarStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var compressedSize, uncompressedSize int64

	for _, col := range c.columns {
		compressedSize += int64(len(col.Compressed))
		uncompressedSize += int64(col.RowCount * 8) // Approximate
	}

	return ColumnarStats{
		ColumnCount:      int64(len(c.columns)),
		BlockCount:       int64(len(c.blocks)),
		RowCount:         c.RowCount(),
		CompressedSize:   compressedSize,
		UncompressedSize: uncompressedSize,
		CompressionRatio: float64(uncompressedSize) / float64(compressedSize+1),
	}
}

// ColumnarStats contains columnar store statistics
type ColumnarStats struct {
	ColumnCount      int64
	BlockCount       int64
	RowCount         int64
	CompressedSize   int64
	UncompressedSize int64
	CompressionRatio float64
}

// Helper functions

func inferColumnType(v interface{}) types.ColumnType {
	switch v.(type) {
	case int, int64, int32:
		return types.TypeInt
	case float64, float32:
		return types.TypeFloat
	case string:
		return types.TypeString
	case []interface{}, map[string]interface{}:
		return types.TypeJSON
	case []float32:
		return types.TypeVector
	default:
		return types.TypeString
	}
}

func encodeValue(w *zstd.Encoder, val interface{}, colType types.ColumnType) error {
	switch colType {
	case types.TypeInt:
		v, _ := val.(int64)
		return binary.Write(w, binary.LittleEndian, v)
	case types.TypeFloat:
		v, _ := val.(float64)
		return binary.Write(w, binary.LittleEndian, v)
	case types.TypeString:
		v, _ := val.(string)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		_, err := w.Write([]byte(v))
		return err
	default:
		return nil
	}
}

func decodeValue(r *zstd.Decoder, colType types.ColumnType) (interface{}, error) {
	switch colType {
	case types.TypeInt:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case types.TypeFloat:
		var v float64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case types.TypeString:
		var len uint32
		if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
			return nil, err
		}
		buf := make([]byte, len)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		return string(buf), nil
	default:
		return nil, nil
	}
}

func compareValues(a, b interface{}) int {
	switch a.(type) {
	case int:
		ai, _ := a.(int)
		bi, _ := b.(int)
		if ai < bi {
			return -1
		} else if ai > bi {
			return 1
		}
	case float64:
		af, _ := a.(float64)
		bf, _ := b.(float64)
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
	case string:
		as, _ := a.(string)
		bs, _ := b.(string)
		if as < bs {
			return -1
		} else if as > bs {
			return 1
		}
	}
	return 0
}

func sumValues(data []interface{}) interface{} {
	var intSum int64
	var floatSum float64
	hasFloat := false

	for _, val := range data {
		switch v := val.(type) {
		case int:
			intSum += int64(v)
		case int64:
			intSum += v
		case int32:
			intSum += int64(v)
		case float64:
			floatSum += v
			hasFloat = true
		case float32:
			floatSum += float64(v)
			hasFloat = true
		}
	}

	if hasFloat {
		return floatSum + float64(intSum)
	}
	return intSum
}
