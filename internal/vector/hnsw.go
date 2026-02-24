package vector

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/thirawat27/kvi/pkg/types"
)

// HNSWIndex implements Hierarchical Navigable Small World algorithm
// for Approximate Nearest Neighbor search
type HNSWIndex struct {
	dim        int   // Dimension (e.g., 384, 768, 1536)
	M          int   // Max connections per element
	efConst    int   // Size of dynamic candidate list
	maxLevel   int   // Current max level
	enterPoint *Node // Entry point to the graph

	nodes      map[string]*Node
	levelCount []int // Count of nodes at each level

	mu sync.RWMutex
}

// Node represents a node in the HNSW graph
type Node struct {
	ID      string           `json:"id"`
	Vector  []float32        `json:"vector"`
	Level   int              `json:"level"`
	Friends map[int][]string `json:"friends"` // layer -> connected node IDs
}

// Candidate for priority queue during search
type Candidate struct {
	ID       string
	Distance float32
}

// MaxHeap for candidates (furthest first - for exclusion)
type MaxHeap []Candidate

func (h MaxHeap) Len() int           { return len(h) }
func (h MaxHeap) Less(i, j int) bool { return h[i].Distance > h[j].Distance }
func (h MaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MaxHeap) Push(x interface{}) {
	*h = append(*h, x.(Candidate))
}

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MinHeap for candidates (closest first - for selection)
type MinHeap []Candidate

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Distance < h[j].Distance }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(Candidate))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewHNSWIndex creates a new HNSW index
func NewHNSWIndex(dim, M, ef int) *HNSWIndex {
	return &HNSWIndex{
		dim:        dim,
		M:          M,
		efConst:    ef,
		maxLevel:   0,
		nodes:      make(map[string]*Node),
		levelCount: make([]int, 16),
	}
}

// Add adds a vector to the index
func (h *HNSWIndex) Add(id string, vector []float32) error {
	if len(vector) != h.dim {
		return fmt.Errorf("dimension mismatch: expected %d, got %d", h.dim, len(vector))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if node already exists
	if _, exists := h.nodes[id]; exists {
		return fmt.Errorf("node %s already exists", id)
	}

	level := h.randomLevel()
	node := &Node{
		ID:      id,
		Vector:  vector,
		Level:   level,
		Friends: make(map[int][]string),
	}

	// First node
	if h.enterPoint == nil {
		h.enterPoint = node
		h.nodes[id] = node
		h.maxLevel = level
		for l := 0; l <= level; l++ {
			h.levelCount[l]++
		}
		return nil
	}

	// Insert into graph
	curr := h.enterPoint
	currDist := CosineDistance(vector, curr.Vector)

	// Traverse from top level down to level+1
	for l := h.maxLevel; l > level; l-- {
		changed := true
		for changed {
			changed = false
			for _, friendID := range curr.Friends[l] {
				if friend, ok := h.nodes[friendID]; ok {
					dist := CosineDistance(vector, friend.Vector)
					if dist < currDist {
						curr = friend
						currDist = dist
						changed = true
					}
				}
			}
		}
	}

	// Insert at levels [0, level]
	for l := min(level, h.maxLevel); l >= 0; l-- {
		candidates := h.searchLayer(vector, curr, h.efConst, l)

		// Select M nearest neighbors
		M := h.M
		if l == 0 {
			M = h.M * 2 // Layer 0 has more connections
		}

		friends := h.selectNeighbors(candidates, M)
		node.Friends[l] = friends

		// Add bidirectional connections
		for _, friendID := range friends {
			if friend, ok := h.nodes[friendID]; ok {
				friend.Friends[l] = append(friend.Friends[l], id)

				// Prune if too many connections
				if l == 0 && len(friend.Friends[l]) > h.M*2 {
					friend.Friends[l] = h.selectNeighborsById(friend.Vector, friend.Friends[l], h.M*2)
				} else if l > 0 && len(friend.Friends[l]) > h.M {
					friend.Friends[l] = h.selectNeighborsById(friend.Vector, friend.Friends[l], h.M)
				}
			}
		}

		// Set next entry point
		if len(candidates) > 0 {
			curr = h.nodes[candidates[0].ID]
		}
	}

	// Update entry point if new node has higher level
	if level > h.maxLevel {
		h.maxLevel = level
		h.enterPoint = node
	}

	h.nodes[id] = node
	for l := 0; l <= level; l++ {
		h.levelCount[l]++
	}

	return nil
}

// Search finds k nearest neighbors
func (h *HNSWIndex) Search(query []float32, k int) ([]string, []float32) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.enterPoint == nil || len(h.nodes) == 0 {
		return nil, nil
	}

	if len(query) != h.dim {
		return nil, nil
	}

	curr := h.enterPoint
	currDist := CosineDistance(query, curr.Vector)

	// Traverse from top level down to level 1
	for l := h.maxLevel; l > 0; l-- {
		changed := true
		for changed {
			changed = false
			for _, friendID := range curr.Friends[l] {
				if friend, ok := h.nodes[friendID]; ok {
					dist := CosineDistance(query, friend.Vector)
					if dist < currDist {
						curr = friend
						currDist = dist
						changed = true
					}
				}
			}
		}
	}

	// Search at layer 0 with ef candidates
	ef := h.efConst
	if ef < k {
		ef = k
	}

	candidates := h.searchLayer(query, curr, ef, 0)

	// Return top k
	results := make([]string, 0, k)
	scores := make([]float32, 0, k)

	for i := 0; i < min(k, len(candidates)); i++ {
		results = append(results, candidates[i].ID)
		scores = append(scores, 1-candidates[i].Distance) // Convert distance to similarity
	}

	return results, scores
}

// Delete removes a node from the index
func (h *HNSWIndex) Delete(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, exists := h.nodes[id]
	if !exists {
		return types.ErrKeyNotFound
	}

	// Remove connections to this node
	for l, friends := range node.Friends {
		for _, friendID := range friends {
			if friend, ok := h.nodes[friendID]; ok {
				newFriends := make([]string, 0)
				for _, fid := range friend.Friends[l] {
					if fid != id {
						newFriends = append(newFriends, fid)
					}
				}
				friend.Friends[l] = newFriends
			}
		}
	}

	// Update entry point if needed
	if h.enterPoint == node {
		// Find a new entry point
		h.enterPoint = nil
		for _, n := range h.nodes {
			if n.ID != id {
				h.enterPoint = n
				break
			}
		}
	}

	delete(h.nodes, id)
	return nil
}

// Get retrieves a node by ID
func (h *HNSWIndex) Get(id string) (*Node, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	node, exists := h.nodes[id]
	if !exists {
		return nil, types.ErrKeyNotFound
	}
	return node, nil
}

// Size returns the number of nodes
func (h *HNSWIndex) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// Stats returns index statistics
func (h *HNSWIndex) Stats() HNSWStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var totalConnections int
	for _, node := range h.nodes {
		for _, friends := range node.Friends {
			totalConnections += len(friends)
		}
	}

	return HNSWStats{
		NodeCount:        len(h.nodes),
		MaxLevel:         h.maxLevel,
		Dimension:        h.dim,
		M:                h.M,
		Ef:               h.efConst,
		TotalConnections: totalConnections,
		AvgConnections:   float64(totalConnections) / float64(len(h.nodes)+1),
	}
}

// HNSWStats contains HNSW index statistics
type HNSWStats struct {
	NodeCount        int
	MaxLevel         int
	Dimension        int
	M                int
	Ef               int
	TotalConnections int
	AvgConnections   float64
}

// Internal methods

func (h *HNSWIndex) randomLevel() int {
	// Geometric distribution with mL = 1/ln(M)
	level := 0
	mL := 1.0 / math.Log(float64(h.M))

	for rand.Float64() < mL && level < 15 {
		level++
	}
	return level
}

func (h *HNSWIndex) searchLayer(query []float32, entry *Node, ef int, level int) []Candidate {
	visited := make(map[string]bool)

	// Min-heap for candidates (to explore)
	candidates := &MinHeap{}
	heap.Init(candidates)

	// Max-heap for results (to keep track of worst)
	results := &MaxHeap{}
	heap.Init(results)

	entryDist := CosineDistance(query, entry.Vector)
	heap.Push(candidates, Candidate{ID: entry.ID, Distance: entryDist})
	heap.Push(results, Candidate{ID: entry.ID, Distance: entryDist})
	visited[entry.ID] = true

	for candidates.Len() > 0 {
		// Get closest candidate
		curr := heap.Pop(candidates).(Candidate)

		// If current is further than worst in results, stop
		if results.Len() > 0 {
			worst := (*results)[0]
			if curr.Distance > worst.Distance {
				break
			}
		}

		// Explore neighbors
		currNode := h.nodes[curr.ID]
		if currNode == nil {
			continue
		}

		for _, friendID := range currNode.Friends[level] {
			if !visited[friendID] {
				visited[friendID] = true

				friend := h.nodes[friendID]
				if friend == nil {
					continue
				}

				dist := CosineDistance(query, friend.Vector)

				if results.Len() < ef || dist < (*results)[0].Distance {
					heap.Push(candidates, Candidate{ID: friendID, Distance: dist})
					heap.Push(results, Candidate{ID: friendID, Distance: dist})

					if results.Len() > ef {
						heap.Pop(results)
					}
				}
			}
		}
	}

	// Convert results to sorted slice
	sorted := make([]Candidate, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(results).(Candidate)
	}

	return sorted
}

func (h *HNSWIndex) selectNeighbors(candidates []Candidate, M int) []string {
	// Simple selection: just take M closest
	result := make([]string, 0, M)
	for i := 0; i < min(M, len(candidates)); i++ {
		result = append(result, candidates[i].ID)
	}
	return result
}

func (h *HNSWIndex) selectNeighborsById(query []float32, ids []string, M int) []string {
	// Calculate distances and sort
	type idDist struct {
		id   string
		dist float32
	}

	pairs := make([]idDist, len(ids))
	for i, id := range ids {
		if node, ok := h.nodes[id]; ok {
			pairs[i] = idDist{id: id, dist: CosineDistance(query, node.Vector)}
		}
	}

	// Sort by distance
	for i := 0; i < len(pairs); i++ {
		for j := i + 1; j < len(pairs); j++ {
			if pairs[j].dist < pairs[i].dist {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
	}

	// Take M closest
	result := make([]string, 0, M)
	for i := 0; i < min(M, len(pairs)); i++ {
		result = append(result, pairs[i].id)
	}
	return result
}

// Distance functions

// CosineDistance calculates 1 - cosine_similarity
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	normProduct := float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB)))
	if normProduct == 0 {
		return 1.0 // Maximum distance
	}

	similarity := dot / normProduct
	return 1.0 - similarity
}

// EuclideanDistance calculates L2 distance
func EuclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// DotProduct calculates dot product similarity
func DotProduct(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// Helper functions

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
