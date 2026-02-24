package engine

import (
	"sync"
	"time"
	"github.com/thirawat27/kvi/pkg/types"
)

// MVCCManager manages multi-version concurrency control
type MVCCManager struct {
	versions map[string][]*VersionedRecord
	mu       sync.RWMutex
	counter  uint64 // Global transaction ID
}

// VersionedRecord represents a versioned record for MVCC
type VersionedRecord struct {
	TxID      uint64
	Record    *types.Record
	Deleted   bool
	Timestamp time.Time
}

// NewMVCCManager creates a new MVCC manager
func NewMVCCManager() *MVCCManager {
	return &MVCCManager{
		versions: make(map[string][]*VersionedRecord),
		counter:  0,
	}
}

// AddVersion adds a new version of a record
func (m *MVCCManager) AddVersion(key string, record *types.Record, txID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vr := &VersionedRecord{
		TxID:      txID,
		Record:    record,
		Deleted:   false,
		Timestamp: time.Now(),
	}

	m.versions[key] = append(m.versions[key], vr)

	// Keep only last 10 versions to prevent memory bloat
	if len(m.versions[key]) > 10 {
		m.versions[key] = m.versions[key][len(m.versions[key])-10:]
	}
}

// MarkDeleted marks a key as deleted at a specific transaction
func (m *MVCCManager) MarkDeleted(key string, txID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vr := &VersionedRecord{
		TxID:      txID,
		Record:    nil,
		Deleted:   true,
		Timestamp: time.Now(),
	}

	m.versions[key] = append(m.versions[key], vr)
}

// Get retrieves a record as of a specific transaction (time-travel query)
func (m *MVCCManager) Get(key string, asOfTx uint64) (*types.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	versions := m.versions[key]
	if len(versions) == 0 {
		return nil, types.ErrKeyNotFound
	}

	// Binary search for the version matching the timestamp
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if v.TxID <= asOfTx && !v.Deleted {
			return v.Record, nil
		}
	}

	return nil, types.ErrKeyNotFound
}

// GetLatest retrieves the latest version of a record
func (m *MVCCManager) GetLatest(key string) (*types.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	versions := m.versions[key]
	if len(versions) == 0 {
		return nil, types.ErrKeyNotFound
	}

	// Find the latest non-deleted version
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if !v.Deleted {
			return v.Record, nil
		}
	}

	return nil, types.ErrKeyNotFound
}

// NextTxID generates the next transaction ID
func (m *MVCCManager) NextTxID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counter++
	return m.counter
}

// Cleanup removes versions older than retention period
func (m *MVCCManager) Cleanup(retention time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-retention)

	for key, versions := range m.versions {
		var newVersions []*VersionedRecord
		for _, v := range versions {
			if v.Timestamp.After(cutoff) {
				newVersions = append(newVersions, v)
			}
		}
		m.versions[key] = newVersions
	}
}

// GetVersionCount returns the number of versions for a key
func (m *MVCCManager) GetVersionCount(key string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.versions[key])
}
