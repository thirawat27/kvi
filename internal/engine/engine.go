package engine

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/thirawat27/kvi/internal/columnar"
	"github.com/thirawat27/kvi/internal/vector"
	"github.com/thirawat27/kvi/internal/wal"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

// KviEngine เป็น implementation หลัก
type KviEngine struct {
	mode types.Mode
	path string // "" if memory-only
	cfg  *config.Config

	// Layer 1: Hot Data (Memory)
	memTable map[string]*types.Record
	memMu    sync.RWMutex

	// Layer 2: Ordered Index (B-tree)
	index   *btree.BTree
	indexMu sync.RWMutex

	// Layer 3: Columnar (for analytics)
	columnar *columnar.ColumnarStore

	// Layer 4: Vector Index (HNSW)
	vectorIndex *vector.HNSWIndex

	// Durability
	wal   *wal.WAL
	walMu sync.Mutex

	// MVCC
	mvcc   *MVCCManager
	mvccMu sync.RWMutex

	// Background
	bgCtx    context.Context
	bgCancel context.CancelFunc
	wg       sync.WaitGroup

	// Stats
	stats EngineStatsInternal
}

// EngineStatsInternal internal statistics
type EngineStatsInternal struct {
	RecordsTotal   int64
	MemoryUsed     int64
	DiskUsed       int64
	CacheHits      int64
	CacheMisses    int64
	AvgQueryTime   time.Duration
	WALSize        int64
	TotalQueries   int64
	TotalQueryTime time.Duration
}

// BTreeItem สำหรับ ordered scan
type BTreeItem struct {
	Key     string
	Version uint64
}

// Less implements btree.Item
func (a BTreeItem) Less(b btree.Item) bool {
	return a.Key < b.(BTreeItem).Key
}

// NewEngine สร้าง engine ใหม่ตาม config
func NewEngine(cfg *config.Config) (*KviEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &KviEngine{
		mode:     cfg.Mode,
		cfg:      cfg,
		memTable: make(map[string]*types.Record, 1024),
		index:    btree.New(32), // Degree 32 = optimal for memory
		mvcc:     NewMVCCManager(),
		bgCtx:    ctx,
		bgCancel: cancel,
	}

	// Initialize mode-specific components
	switch cfg.Mode {
	case types.ModeMemory:
		// Pure memory mode - no additional setup needed
	case types.ModeDisk, types.ModeHybrid:
		if cfg.EnableWAL && cfg.WALPath != "" {
			w, err := wal.OpenWAL(cfg.WALPath)
			if err != nil {
				return nil, err
			}
			e.wal = w
			// Recovery from WAL
			if err := e.recoverFromWAL(); err != nil {
				return nil, err
			}
		}
	case types.ModeColumnar:
		e.columnar = columnar.NewColumnarStore(cfg.ColumnarBlockSize, cfg.Compression)
	case types.ModeVector:
		e.vectorIndex = vector.NewHNSWIndex(cfg.VectorDimensions, cfg.HNSWM, cfg.HNSWEf)
	}

	// Start background tasks
	e.wg.Add(1)
	go e.ttlCleaner()

	e.wg.Add(1)
	go e.statsCollector()

	return e, nil
}

// NewMemoryEngine สร้าง engine ที่เร็วที่สุด ไม่มี persistence
func NewMemoryEngine() (*KviEngine, error) {
	return NewEngine(config.MemoryConfig())
}

// NewDiskEngine สร้าง engine พร้อม persistence
func NewDiskEngine(dataDir string) (*KviEngine, error) {
	cfg := config.DiskConfig()
	cfg.DataDir = dataDir
	cfg.WALPath = dataDir + "/wal.log"
	cfg.SnapshotDir = dataDir + "/snapshots"
	return NewEngine(cfg)
}

// Get retrieves a record by key
func (e *KviEngine) Get(ctx context.Context, key string) (*types.Record, error) {
	start := time.Now()
	defer e.updateStats(time.Since(start))

	e.memMu.RLock()
	rec, exists := e.memTable[key]
	e.memMu.RUnlock()

	if !exists {
		e.stats.CacheMisses++
		return nil, types.ErrKeyNotFound
	}

	// Check TTL (Redis-style expiration)
	if rec.TTL != nil && time.Now().After(*rec.TTL) {
		e.stats.CacheMisses++
		return nil, types.ErrKeyNotFound
	}

	// Verify checksum (optional for memory mode)
	if e.mode != types.ModeMemory && e.cfg.EnableChecksum {
		if !verifyChecksum(rec) {
			return nil, types.ErrDataCorruption
		}
	}

	e.stats.CacheHits++
	return rec, nil
}

// Put stores a record
func (e *KviEngine) Put(ctx context.Context, key string, value *types.Record) error {
	start := time.Now()
	defer e.updateStats(time.Since(start))

	if value.ID == "" {
		value.ID = key
	}

	now := time.Now()
	if value.CreatedAt.IsZero() {
		value.CreatedAt = now
	}
	value.UpdatedAt = now
	value.Version = uint64(now.UnixNano())
	value.Checksum = calculateChecksum(value)

	// Write to WAL first (if enabled)
	if e.wal != nil {
		if err := e.wal.Append(&wal.LogEntry{
			Op:     types.OpPut,
			Key:    key,
			Record: value,
		}); err != nil {
			return err
		}
	}

	// Update MVCC
	e.mvccMu.Lock()
	e.mvcc.AddVersion(key, value, value.Version)
	e.mvccMu.Unlock()

	// Update memory
	e.memMu.Lock()
	e.memTable[key] = value
	e.stats.RecordsTotal = int64(len(e.memTable))
	e.memMu.Unlock()

	// Update index (async for non-strict consistency)
	e.indexMu.Lock()
	e.index.ReplaceOrInsert(BTreeItem{Key: key, Version: value.Version})
	e.indexMu.Unlock()

	// Update columnar store if enabled
	if e.columnar != nil {
		e.columnar.InsertBatch([]*types.Record{value})
	}

	// Update vector index if record has vector
	if e.vectorIndex != nil && len(value.Vector) > 0 {
		if err := e.vectorIndex.Add(key, value.Vector); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes a record
func (e *KviEngine) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer e.updateStats(time.Since(start))

	// Write to WAL first
	if e.wal != nil {
		if err := e.wal.Append(&wal.LogEntry{
			Op:  types.OpDelete,
			Key: key,
		}); err != nil {
			return err
		}
	}

	// Update MVCC
	e.mvccMu.Lock()
	e.mvcc.MarkDeleted(key, uint64(time.Now().UnixNano()))
	e.mvccMu.Unlock()

	// Remove from memory
	e.memMu.Lock()
	delete(e.memTable, key)
	e.stats.RecordsTotal = int64(len(e.memTable))
	e.memMu.Unlock()

	// Remove from index
	e.indexMu.Lock()
	e.index.Delete(BTreeItem{Key: key})
	e.indexMu.Unlock()

	return nil
}

// Scan retrieves records in key range
func (e *KviEngine) Scan(ctx context.Context, start, end string, limit int) ([]*types.Record, error) {
	startTime := time.Now()
	defer e.updateStats(time.Since(startTime))

	var records []*types.Record
	count := 0

	e.indexMu.RLock()
	defer e.indexMu.RUnlock()

	e.index.AscendGreaterOrEqual(BTreeItem{Key: start}, func(item btree.Item) bool {
		btItem := item.(BTreeItem)
		if end != "" && btItem.Key >= end {
			return false
		}
		if limit > 0 && count >= limit {
			return false
		}

		e.memMu.RLock()
		rec, exists := e.memTable[btItem.Key]
		e.memMu.RUnlock()

		if exists && (rec.TTL == nil || !time.Now().After(*rec.TTL)) {
			records = append(records, rec)
			count++
		}
		return true
	})

	return records, nil
}

// BatchPut stores multiple records efficiently
func (e *KviEngine) BatchPut(ctx context.Context, entries map[string]*types.Record) error {
	start := time.Now()
	defer e.updateStats(time.Since(start))

	// Write to WAL
	if e.wal != nil {
		for key, value := range entries {
			if value.ID == "" {
				value.ID = key
			}
			now := time.Now()
			if value.CreatedAt.IsZero() {
				value.CreatedAt = now
			}
			value.UpdatedAt = now
			value.Version = uint64(now.UnixNano())
			value.Checksum = calculateChecksum(value)
		}

		if err := e.wal.Append(&wal.LogEntry{
			Op:     types.OpBatch,
			Record: &types.Record{Data: map[string]interface{}{"batch": entries}},
		}); err != nil {
			return err
		}
	}

	// Batch insert
	e.memMu.Lock()
	for key, value := range entries {
		e.memTable[key] = value

		e.indexMu.Lock()
		e.index.ReplaceOrInsert(BTreeItem{Key: key, Version: value.Version})
		e.indexMu.Unlock()
	}
	e.memMu.Unlock()

	// Update columnar if enabled
	if e.columnar != nil {
		records := make([]*types.Record, 0, len(entries))
		for _, v := range entries {
			records = append(records, v)
		}
		e.columnar.InsertBatch(records)
	}

	return nil
}

// Snapshot creates a point-in-time snapshot
func (e *KviEngine) Snapshot() (types.Snapshot, error) {
	e.memMu.RLock()
	defer e.memMu.RUnlock()

	records := make(map[string]*types.Record, len(e.memTable))
	for k, v := range e.memTable {
		// Create a copy of the record
		recordCopy := *v
		records[k] = &recordCopy
	}

	snap := types.Snapshot{
		Version:   uint64(time.Now().UnixNano()),
		Records:   records,
		CreatedAt: time.Now(),
	}

	// Calculate checksum after all data is set
	snap.Checksum = calculateSnapshotChecksum(snap)

	return snap, nil
}

// Restore restores from a snapshot
func (e *KviEngine) Restore(snap types.Snapshot) error {
	// Verify checksum
	expectedChecksum := calculateSnapshotChecksum(snap)
	if snap.Checksum != expectedChecksum {
		return types.ErrDataCorruption
	}

	e.memMu.Lock()
	e.memTable = make(map[string]*types.Record, len(snap.Records))
	for k, v := range snap.Records {
		e.memTable[k] = v
	}
	e.stats.RecordsTotal = int64(len(e.memTable))
	e.memMu.Unlock()

	// Rebuild index
	e.indexMu.Lock()
	e.index = btree.New(32)
	for k, v := range snap.Records {
		e.index.ReplaceOrInsert(BTreeItem{Key: k, Version: v.Version})
	}
	e.indexMu.Unlock()

	return nil
}

// Close shuts down the engine
func (e *KviEngine) Close() error {
	e.bgCancel()
	e.wg.Wait()

	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns engine statistics
func (e *KviEngine) Stats() types.EngineStats {
	total := e.stats.CacheHits + e.stats.CacheMisses
	hitRatio := float64(0)
	if total > 0 {
		hitRatio = float64(e.stats.CacheHits) / float64(total)
	}

	avgTime := time.Duration(0)
	if e.stats.TotalQueries > 0 {
		avgTime = time.Duration(int64(e.stats.TotalQueryTime) / e.stats.TotalQueries)
	}

	return types.EngineStats{
		RecordsTotal:  e.stats.RecordsTotal,
		MemoryUsed:    e.stats.MemoryUsed,
		DiskUsed:      e.stats.DiskUsed,
		CacheHitRatio: hitRatio,
		AvgQueryTime:  avgTime,
		WALSize:       e.stats.WALSize,
	}
}

// VectorSearch performs similarity search
func (e *KviEngine) VectorSearch(query []float32, k int) ([]string, []float32, error) {
	if e.vectorIndex == nil {
		return nil, nil, types.ErrInvalidMode
	}
	ids, scores := e.vectorIndex.Search(query, k)
	return ids, scores, nil
}

// GetAsOf retrieves a record as of a specific version (time-travel query)
func (e *KviEngine) GetAsOf(key string, asOfTx uint64) (*types.Record, error) {
	return e.mvcc.Get(key, asOfTx)
}

// Internal methods

func (e *KviEngine) updateStats(d time.Duration) {
	e.stats.TotalQueries++
	e.stats.TotalQueryTime += d
}

func (e *KviEngine) ttlCleaner() {
	defer e.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-e.bgCtx.Done():
			return
		case <-ticker.C:
			e.cleanExpired()
		}
	}
}

func (e *KviEngine) cleanExpired() {
	now := time.Now()
	var expiredKeys []string

	e.memMu.RLock()
	for k, v := range e.memTable {
		if v.TTL != nil && now.After(*v.TTL) {
			expiredKeys = append(expiredKeys, k)
		}
	}
	e.memMu.RUnlock()

	for _, k := range expiredKeys {
		e.Delete(e.bgCtx, k)
	}
}

func (e *KviEngine) statsCollector() {
	defer e.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.bgCtx.Done():
			return
		case <-ticker.C:
			e.memMu.RLock()
			e.stats.RecordsTotal = int64(len(e.memTable))
			e.memMu.RUnlock()

			if e.wal != nil {
				e.stats.WALSize = e.wal.Size()
			}
		}
	}
}

func (e *KviEngine) recoverFromWAL() error {
	if e.wal == nil {
		return nil
	}

	entries, err := e.wal.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Op {
		case types.OpPut:
			e.memMu.Lock()
			e.memTable[entry.Key] = entry.Record
			e.memMu.Unlock()
		case types.OpDelete:
			e.memMu.Lock()
			delete(e.memTable, entry.Key)
			e.memMu.Unlock()
		}
	}

	return nil
}

// Helper functions

func calculateChecksum(r *types.Record) uint32 {
	data := r.ID + r.UpdatedAt.String()
	for k, v := range r.Data {
		data += k + toString(v)
	}
	return crc32.ChecksumIEEE([]byte(data))
}

func verifyChecksum(r *types.Record) bool {
	return r.Checksum == calculateChecksum(r)
}

func calculateMapChecksum(records map[string]*types.Record) uint32 {
	var data string
	for k, v := range records {
		data += k + v.ID
	}
	return crc32.ChecksumIEEE([]byte(data))
}

func calculateSnapshotChecksum(snap types.Snapshot) uint32 {
	// Only use the count of records for checksum (avoid map iteration order issues)
	data := fmt.Sprintf("%d|%d|%d|", snap.Version, snap.CreatedAt.UnixNano(), len(snap.Records))
	return crc32.ChecksumIEEE([]byte(data))
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return string(rune(val))
	case float64:
		return string(rune(int(val)))
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func inferType(v interface{}) types.ColumnType {
	switch v.(type) {
	case int, int64, int32:
		return types.TypeInt
	case float64, float32:
		return types.TypeFloat
	case string:
		return types.TypeString
	case []interface{}:
		return types.TypeJSON
	case []float32:
		return types.TypeVector
	default:
		return types.TypeString
	}
}

func randomLevel() int {
	level := 0
	for rand.Float64() < 0.5 && level < 16 {
		level++
	}
	return level
}
