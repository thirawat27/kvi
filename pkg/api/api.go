package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/thirawat27/kvi/internal/engine"
	"github.com/thirawat27/kvi/internal/pubsub"
	"github.com/thirawat27/kvi/internal/sql"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

// Server represents the HTTP API server
type Server struct {
	engine *engine.KviEngine
	hub    *pubsub.Hub
	parser *sql.Parser
	config *config.Config
	server *http.Server
}

// NewServer creates a new API server
func NewServer(eng *engine.KviEngine, cfg *config.Config) *Server {
	return &Server{
		engine: eng,
		hub:    pubsub.NewHub(),
		parser: sql.NewParser(),
		config: cfg,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// CRUD endpoints
	mux.HandleFunc("/api/v1/get", s.handleGet)
	mux.HandleFunc("/api/v1/put", s.handlePut)
	mux.HandleFunc("/api/v1/delete", s.handleDelete)
	mux.HandleFunc("/api/v1/scan", s.handleScan)
	mux.HandleFunc("/api/v1/batch", s.handleBatch)

	// Query endpoint
	mux.HandleFunc("/api/v1/query", s.handleQuery)

	// Vector endpoints
	mux.HandleFunc("/api/v1/vector/add", s.handleVectorAdd)
	mux.HandleFunc("/api/v1/vector/search", s.handleVectorSearch)

	// Pub/Sub endpoints
	mux.HandleFunc("/api/v1/pub", s.handlePublish)
	mux.HandleFunc("/api/v1/sub", s.handleSubscribe)

	// Stats endpoint
	mux.HandleFunc("/api/v1/stats", s.handleStats)

	// Health check
	mux.HandleFunc("/health", s.handleHealth)

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Response types

// APIResponse is the standard API response
type APIResponse struct {
	Success   bool        `json:"success"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// ErrorResponse returns an error response
func (s *Server) ErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(APIResponse{
		Success:   false,
		Error:     message,
		Timestamp: time.Now(),
	})
}

// SuccessResponse returns a success response
func (s *Server) SuccessResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(APIResponse{
		Success:   true,
		Data:      data,
		Timestamp: time.Now(),
	})
}

// Handlers

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		s.ErrorResponse(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	// Check for as_of parameter (time-travel query)
	asOfStr := r.URL.Query().Get("as_of")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var record *types.Record
	var err error

	if asOfStr != "" {
		asOf, parseErr := strconv.ParseUint(asOfStr, 10, 64)
		if parseErr != nil {
			s.ErrorResponse(w, "Invalid as_of parameter", http.StatusBadRequest)
			return
		}
		record, err = s.engine.GetAsOf(key, asOf)
	} else {
		record, err = s.engine.Get(ctx, key)
	}

	if err != nil {
		if err == types.ErrKeyNotFound {
			s.ErrorResponse(w, "Key not found", http.StatusNotFound)
			return
		}
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, record)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key    string                 `json:"key"`
		Data   map[string]interface{} `json:"data"`
		Vector []float32              `json:"vector,omitempty"`
		TTL    *time.Time             `json:"ttl,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.ErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		s.ErrorResponse(w, "Missing key", http.StatusBadRequest)
		return
	}

	record := &types.Record{
		ID:     req.Key,
		Data:   req.Data,
		Vector: req.Vector,
		TTL:    req.TTL,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.engine.Put(ctx, req.Key, record); err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, map[string]string{"key": req.Key})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		s.ErrorResponse(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.engine.Delete(ctx, key); err != nil {
		if err == types.ErrKeyNotFound {
			s.ErrorResponse(w, "Key not found", http.StatusNotFound)
			return
		}
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, map[string]bool{"deleted": true})
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	records, err := s.engine.Scan(ctx, start, end, limit)
	if err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, records)
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Entries map[string]*types.Record `json:"entries"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.ErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.engine.BatchPut(ctx, req.Entries); err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, map[string]int{"count": len(req.Entries)})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Query string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.ErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	stmt, err := s.parser.Parse(req.Query)
	if err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Execute parsed statement
	switch stmt.Type {
	case sql.Select:
		s.executeSelect(w, r, stmt)
	case sql.Insert:
		s.executeInsert(w, r, stmt)
	case sql.Delete:
		s.executeDelete(w, r, stmt)
	case sql.VectorSearch:
		s.executeVectorSearch(w, r, stmt)
	default:
		s.ErrorResponse(w, "Unsupported statement type", http.StatusBadRequest)
	}
}

func (s *Server) executeSelect(w http.ResponseWriter, r *http.Request, stmt *sql.Statement) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if stmt.Where != nil {
		// Single key lookup
		if stmt.Where.Column == "id" || stmt.Where.Column == "key" {
			key := fmt.Sprintf("%v", stmt.Where.Value)
			record, err := s.engine.Get(ctx, key)
			if err != nil {
				if err == types.ErrKeyNotFound {
					s.SuccessResponse(w, []interface{}{})
					return
				}
				s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
				return
			}
			s.SuccessResponse(w, []*types.Record{record})
			return
		}
	}

	// Range scan
	limit := stmt.Limit
	if limit <= 0 {
		limit = 100
	}

	records, err := s.engine.Scan(ctx, "", "", limit)
	if err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, records)
}

func (s *Server) executeInsert(w http.ResponseWriter, r *http.Request, stmt *sql.Statement) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	record := &types.Record{
		Data: make(map[string]interface{}),
	}

	for i, col := range stmt.Columns {
		if i < len(stmt.Values) {
			record.Data[col] = stmt.Values[i]
		}
	}

	// Generate key from first column or use table name
	key := stmt.Table
	if len(stmt.Columns) > 0 && len(stmt.Values) > 0 {
		key = fmt.Sprintf("%v", stmt.Values[0])
	}
	record.ID = key

	if err := s.engine.Put(ctx, key, record); err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, map[string]string{"key": key})
}

func (s *Server) executeDelete(w http.ResponseWriter, r *http.Request, stmt *sql.Statement) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if stmt.Where != nil && (stmt.Where.Column == "id" || stmt.Where.Column == "key") {
		key := fmt.Sprintf("%v", stmt.Where.Value)
		if err := s.engine.Delete(ctx, key); err != nil {
			s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.SuccessResponse(w, map[string]bool{"deleted": true})
		return
	}

	s.ErrorResponse(w, "DELETE requires WHERE clause on id/key", http.StatusBadRequest)
}

func (s *Server) executeVectorSearch(w http.ResponseWriter, r *http.Request, stmt *sql.Statement) {
	if stmt.VectorQuery == nil {
		s.ErrorResponse(w, "Invalid vector search query", http.StatusBadRequest)
		return
	}

	ids, scores, err := s.engine.VectorSearch(stmt.VectorQuery.Vector, stmt.VectorQuery.K)
	if err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	results := make([]map[string]interface{}, len(ids))
	for i, id := range ids {
		results[i] = map[string]interface{}{
			"id":    id,
			"score": scores[i],
		}
	}

	s.SuccessResponse(w, results)
}

func (s *Server) handleVectorAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key    string                 `json:"key"`
		Vector []float32              `json:"vector"`
		Data   map[string]interface{} `json:"data,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.ErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Key == "" || len(req.Vector) == 0 {
		s.ErrorResponse(w, "Missing key or vector", http.StatusBadRequest)
		return
	}

	record := &types.Record{
		ID:     req.Key,
		Vector: req.Vector,
		Data:   req.Data,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.engine.Put(ctx, req.Key, record); err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, map[string]string{"key": req.Key, "dimensions": strconv.Itoa(len(req.Vector))})
}

func (s *Server) handleVectorSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Vector []float32 `json:"vector"`
		K      int       `json:"k"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.ErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if len(req.Vector) == 0 {
		s.ErrorResponse(w, "Missing vector", http.StatusBadRequest)
		return
	}

	if req.K <= 0 {
		req.K = 10
	}

	ids, scores, err := s.engine.VectorSearch(req.Vector, req.K)
	if err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	results := make([]map[string]interface{}, len(ids))
	for i, id := range ids {
		results[i] = map[string]interface{}{
			"id":    id,
			"score": scores[i],
		}
	}

	s.SuccessResponse(w, results)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Channel string      `json:"channel"`
		Data    interface{} `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.ErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Channel == "" {
		s.ErrorResponse(w, "Missing channel", http.StatusBadRequest)
		return
	}

	// Create channel if not exists
	if _, err := s.hub.GetChannel(req.Channel); err != nil {
		if _, createErr := s.hub.CreateChannel(req.Channel, 100); createErr != nil {
			s.ErrorResponse(w, createErr.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := s.hub.Publish(req.Channel, req.Data); err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.SuccessResponse(w, map[string]bool{"published": true})
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	channel := r.URL.Query().Get("channel")
	subscriberID := r.URL.Query().Get("id")

	if channel == "" || subscriberID == "" {
		s.ErrorResponse(w, "Missing channel or id parameter", http.StatusBadRequest)
		return
	}

	// Check if channel exists, create if not
	if _, err := s.hub.GetChannel(channel); err != nil {
		if _, createErr := s.hub.CreateChannel(channel, 100); createErr != nil {
			s.ErrorResponse(w, createErr.Error(), http.StatusInternalServerError)
			return
		}
	}

	sub, err := s.hub.Subscribe(channel, subscriberID)
	if err != nil {
		s.ErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.ErrorResponse(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Send messages
	for {
		select {
		case <-r.Context().Done():
			s.hub.Unsubscribe(channel, subscriberID)
			return
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.ErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.engine.Stats()
	pubsubStats := s.hub.Stats()

	response := map[string]interface{}{
		"engine":  stats,
		"pubsub":  pubsubStats,
		"mode":    s.config.Mode.String(),
		"version": "1.0.0",
	}

	s.SuccessResponse(w, response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"mode":      s.config.Mode.String(),
	})
}

// Query helper for raw queries
func (s *Server) Query(query string) (interface{}, error) {
	stmt, err := s.parser.Parse(query)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	switch stmt.Type {
	case sql.Select:
		if stmt.Where != nil && (stmt.Where.Column == "id" || stmt.Where.Column == "key") {
			key := fmt.Sprintf("%v", stmt.Where.Value)
			return s.engine.Get(ctx, key)
		}
		return s.engine.Scan(ctx, "", "", stmt.Limit)

	case sql.Insert:
		record := &types.Record{Data: make(map[string]interface{})}
		for i, col := range stmt.Columns {
			if i < len(stmt.Values) {
				record.Data[col] = stmt.Values[i]
			}
		}
		key := stmt.Table
		if len(stmt.Columns) > 0 && len(stmt.Values) > 0 {
			key = fmt.Sprintf("%v", stmt.Values[0])
		}
		record.ID = key
		return nil, s.engine.Put(ctx, key, record)

	case sql.Delete:
		if stmt.Where != nil && (strings.ToLower(stmt.Where.Column) == "id" || strings.ToLower(stmt.Where.Column) == "key") {
			key := fmt.Sprintf("%v", stmt.Where.Value)
			return nil, s.engine.Delete(ctx, key)
		}
		return nil, fmt.Errorf("DELETE requires WHERE clause on id/key")

	case sql.VectorSearch:
		if stmt.VectorQuery != nil {
			ids, scores, err := s.engine.VectorSearch(stmt.VectorQuery.Vector, stmt.VectorQuery.K)
			if err != nil {
				return nil, err
			}
			results := make([]map[string]interface{}, len(ids))
			for i, id := range ids {
				results[i] = map[string]interface{}{"id": id, "score": scores[i]}
			}
			return results, nil
		}
	}

	return nil, fmt.Errorf("unsupported statement type")
}
