package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/thirawat27/kvi/internal/engine"
	"github.com/thirawat27/kvi/internal/sql"
	"github.com/thirawat27/kvi/pkg/api"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

var (
	version = "1.0.0"
)

func main() {
	// Define flags
	var (
		mode        = flag.String("mode", "hybrid", "Engine mode: memory, disk, columnar, vector, hybrid")
		dir         = flag.String("dir", "", "Data directory (for disk/hybrid mode)")
		port        = flag.Int("port", 8080, "HTTP server port")
		host        = flag.String("host", "localhost", "HTTP server host")
		vectorDim   = flag.Int("vector-dim", 384, "Vector dimensions (for vector mode)")
		maxMemory   = flag.Int("max-memory", 1024, "Max memory in MB")
		enableWAL   = flag.Bool("wal", true, "Enable Write-Ahead Logging")
		showVersion = flag.Bool("version", false, "Show version")
		query       = flag.String("query", "", "Execute query and exit")
		backup      = flag.String("backup", "", "Create backup to file")
		restore     = flag.String("restore", "", "Restore from backup file")
	)
	flag.Parse()

	// Show version
	if *showVersion {
		fmt.Printf("Kvi v%s\n", version)
		os.Exit(0)
	}

	// Build config
	cfg := config.DefaultConfig()
	cfg.Host = *host
	cfg.Port = *port
	cfg.MaxMemoryMB = *maxMemory
	cfg.EnableWAL = *enableWAL
	cfg.VectorDimensions = *vectorDim

	// Set mode
	switch strings.ToLower(*mode) {
	case "memory":
		cfg.Mode = types.ModeMemory
		cfg.EnableWAL = false
	case "disk":
		cfg.Mode = types.ModeDisk
		if *dir != "" {
			cfg.DataDir = *dir
			cfg.WALPath = *dir + "/wal.log"
		}
	case "columnar":
		cfg.Mode = types.ModeColumnar
	case "vector":
		cfg.Mode = types.ModeVector
		cfg.VectorDimensions = *vectorDim
	case "hybrid":
		cfg.Mode = types.ModeHybrid
		if *dir != "" {
			cfg.DataDir = *dir
			cfg.WALPath = *dir + "/wal.log"
		}
	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}

	// Create engine
	eng, err := engine.NewEngine(cfg)
	if err != nil {
		log.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Handle backup
	if *backup != "" {
		if err := createBackup(eng, *backup); err != nil {
			log.Fatalf("Backup failed: %v", err)
		}
		fmt.Printf("Backup created: %s\n", *backup)
		os.Exit(0)
	}

	// Handle restore
	if *restore != "" {
		if err := restoreBackup(eng, *restore); err != nil {
			log.Fatalf("Restore failed: %v", err)
		}
		fmt.Printf("Restored from: %s\n", *restore)
		os.Exit(0)
	}

	// Handle single query
	if *query != "" {
		result, err := executeQuery(eng, *query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if result != nil {
			data, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(data))
		}
		os.Exit(0)
	}

	// Start server
	server := api.NewServer(eng, cfg)

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(ctx)
		eng.Close()
		os.Exit(0)
	}()

	log.Printf("Kvi v%s starting...", version)
	log.Printf("Mode: %s", cfg.Mode.String())
	log.Printf("Listening on %s:%d", cfg.Host, cfg.Port)

	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func createBackup(eng *engine.KviEngine, path string) error {
	snap, err := eng.Snapshot()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func restoreBackup(eng *engine.KviEngine, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var snap types.Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	return eng.Restore(snap)
}

func executeQuery(eng *engine.KviEngine, query string) (interface{}, error) {
	parser := sql.NewParser()
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	switch stmt.Type {
	case sql.Select:
		if stmt.Where != nil && (stmt.Where.Column == "id" || stmt.Where.Column == "key") {
			key := fmt.Sprintf("%v", stmt.Where.Value)
			return eng.Get(ctx, key)
		}
		return eng.Scan(ctx, "", "", stmt.Limit)

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
		return map[string]string{"key": key}, eng.Put(ctx, key, record)

	case sql.Delete:
		if stmt.Where != nil && (strings.ToLower(stmt.Where.Column) == "id" || strings.ToLower(stmt.Where.Column) == "key") {
			key := fmt.Sprintf("%v", stmt.Where.Value)
			return map[string]bool{"deleted": true}, eng.Delete(ctx, key)
		}
		return nil, fmt.Errorf("DELETE requires WHERE clause on id/key")

	case sql.VectorSearch:
		if stmt.VectorQuery != nil {
			ids, scores, err := eng.VectorSearch(stmt.VectorQuery.Vector, stmt.VectorQuery.K)
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
