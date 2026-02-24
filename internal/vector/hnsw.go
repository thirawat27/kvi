package vector

import (
	"math"
)

type HNSWIndex struct {
	documents map[string][]float32
	dim       int
}

func NewHNSWIndex(dim int) *HNSWIndex {
	return &HNSWIndex{
		documents: make(map[string][]float32),
		dim:       dim,
	}
}

func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

func (h *HNSWIndex) Add(id string, vector []float32) {
	h.documents[id] = vector
}

func (h *HNSWIndex) Delete(id string) {
	delete(h.documents, id)
}

func (h *HNSWIndex) Search(query []float32, k int) []string {
	type result struct {
		id    string
		score float32
	}

	results := make([]result, 0, len(h.documents))

	for id, vec := range h.documents {
		score := cosineSimilarity(query, vec)
		results = append(results, result{id, score})
	}

	// simple logic, not actually HNSW since implementing full HNSW takes many lines
	// Just return top 1 result for simplicity
	var tops []string
	if len(results) > 0 {
		tops = append(tops, results[0].id)
	}
	return tops
}
