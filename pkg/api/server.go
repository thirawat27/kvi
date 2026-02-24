package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/thirawat27/kvi/internal/pubsub"
	"github.com/thirawat27/kvi/internal/sql"
	"github.com/thirawat27/kvi/pkg/types"
)

type Server struct {
	engine    types.Engine
	hub       *pubsub.Hub
	executor  *sql.Executor
	startTime time.Time
	authOn    bool // set to true to require JWT on all routes
}

func NewServer(eng types.Engine, opts ...func(*Server)) *Server {
	s := &Server{
		engine:    eng,
		hub:       pubsub.NewHub(),
		executor:  sql.NewExecutor(eng),
		startTime: time.Now(),
		authOn:    false,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// WithAuth enables JWT authentication on all routes except /health and /api/v1/auth.
func WithAuth() func(*Server) {
	return func(s *Server) { s.authOn = true }
}

// cors is a simple middleware that adds CORS headers.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) wrap(h http.HandlerFunc) http.HandlerFunc {
	if s.authOn {
		return s.authMiddleware(h)
	}
	return h
}

func (s *Server) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/auth", s.handleAuth)
	mux.HandleFunc("/api/v1/get", s.wrap(s.handleGet))
	mux.HandleFunc("/api/v1/put", s.wrap(s.handlePut))
	mux.HandleFunc("/api/v1/delete", s.wrap(s.handleDelete))
	mux.HandleFunc("/api/v1/query", s.wrap(s.handleQuery))
	mux.HandleFunc("/api/v1/pub", s.wrap(s.handlePub))
	mux.HandleFunc("/api/v1/sub", s.wrap(s.handleSub)) // SSE
	mux.HandleFunc("/api/v1/stats", s.wrap(s.handleStats))
	mux.HandleFunc("/health", s.handleHealth)
}

// ── GET ──────────────────────────────────────────────────────────────────────

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"missing 'key' query parameter"}`, http.StatusBadRequest)
		return
	}
	record, err := s.engine.Get(r.Context(), key)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusNotFound)
		return
	}
	jsonOK(w, record)
}

// ── PUT ──────────────────────────────────────────────────────────────────────

type putRequest struct {
	Key  string                 `json:"key"`
	Data map[string]interface{} `json:"data"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, `{"error":"key is required"}`, http.StatusBadRequest)
		return
	}
	record := &types.Record{ID: req.Key, Data: req.Data}
	if err := s.engine.Put(r.Context(), req.Key, record); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	jsonOK(w, map[string]string{"status": "ok", "key": req.Key})
}

// ── DELETE ───────────────────────────────────────────────────────────────────

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"missing 'key' query parameter"}`, http.StatusBadRequest)
		return
	}
	if err := s.engine.Delete(r.Context(), key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]string{"status": "ok", "deleted_key": key})
}

// ── SQL QUERY ────────────────────────────────────────────────────────────────

type queryRequest struct {
	Query string `json:"query"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.executor.ExecuteQuery(r.Context(), req.Query)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}
	jsonOK(w, result)
}

// ── PUB/SUB ──────────────────────────────────────────────────────────────────

type pubRequest struct {
	Channel string `json:"channel"`
	Message string `json:"message"`
}

func (s *Server) handlePub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pubRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	count := s.hub.Publish(req.Channel, req.Message)
	jsonOK(w, map[string]interface{}{"status": "ok", "receivers": count})
}

// handleSub registers an SSE subscriber and streams pub/sub messages.
func (s *Server) handleSub(w http.ResponseWriter, r *http.Request) {
	channel := r.URL.Query().Get("channel")
	subID := r.URL.Query().Get("id")
	if channel == "" || subID == "" {
		http.Error(w, `{"error":"channel and id query params required"}`, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	sub := s.hub.Subscribe(channel, subID)
	defer s.hub.Unsubscribe(channel, subID)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, open := <-sub.C:
			if !open {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
			flusher.Flush()
		}
	}
}

// ── STATS ─────────────────────────────────────────────────────────────────────

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	uptime := time.Since(s.startTime).Truncate(time.Second)
	jsonOK(w, map[string]interface{}{
		"uptime_seconds":  uptime.Seconds(),
		"goroutines":      runtime.NumGoroutine(),
		"mem_alloc_bytes": mem.Alloc,
		"mem_total_bytes": mem.TotalAlloc,
		"mem_sys_bytes":   mem.Sys,
		"gc_cycles":       mem.NumGC,
	})
}

// ── HEALTH ────────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, map[string]string{"status": "ok", "engine": "kvi"})
}

// ── START ─────────────────────────────────────────────────────────────────────

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	s.RegisterHandlers(mux)
	srv := &http.Server{
		Addr:         addr,
		Handler:      cors(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return srv.ListenAndServe()
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
