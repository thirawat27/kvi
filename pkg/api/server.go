package api

import (
	"encoding/json"
	"net/http"

	"github.com/thirawat27/kvi/internal/pubsub"
	"github.com/thirawat27/kvi/internal/sql"
	"github.com/thirawat27/kvi/pkg/types"
)

type Server struct {
	engine   types.Engine
	hub      *pubsub.Hub
	executor *sql.Executor
}

func NewServer(eng types.Engine) *Server {
	return &Server{
		engine:   eng,
		hub:      pubsub.NewHub(),
		executor: sql.NewExecutor(eng),
	}
}

func (s *Server) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/get", s.handleGet)
	mux.HandleFunc("/api/v1/put", s.handlePut)
	mux.HandleFunc("/api/v1/query", s.handleQuery)
	mux.HandleFunc("/api/v1/pub", s.handlePub)
	mux.HandleFunc("/health", s.handleHealth)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key validation", http.StatusBadRequest)
		return
	}

	record, err := s.engine.Get(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(record)
}

type putRequest struct {
	Key    string                 `json:"key"`
	Record map[string]interface{} `json:"data"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record := &types.Record{
		ID:   req.Key,
		Data: req.Record,
	}

	if err := s.engine.Put(r.Context(), req.Key, record); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type queryRequest struct {
	Query string `json:"query"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := s.executor.ExecuteQuery(r.Context(), req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

type pubRequest struct {
	Channel string `json:"channel"`
	Message string `json:"message"`
}

func (s *Server) handlePub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pubRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	count := s.hub.Publish(req.Channel, req.Message)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "receivers": count})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	s.RegisterHandlers(mux)

	// Add CORS implicitly via middleware or direct header sets
	// Mock simply:
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return server.ListenAndServe()
}
