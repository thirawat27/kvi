package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/thirawat27/kvi/pkg/api"
	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/kvi"
	"github.com/thirawat27/kvi/pkg/types"
)

func main() {
	modeStr := flag.String("mode", string(types.ModeHybrid), "Engine mode: memory, disk, columnar, vector, hybrid")
	dataDir := flag.String("dir", "./data", "Data directory for disk operations")
	port := flag.Int("port", 8080, "REST API port")
	grpcPort := flag.Int("grpc-port", 50051, "gRPC port")

	flag.Parse()

	cfg := config.DefaultConfig()
	cfg.Mode = types.Mode(*modeStr)
	cfg.DataDir = *dataDir
	cfg.Port = *port
	cfg.GrpcPort = *grpcPort

	// Set configuration according to mode
	switch cfg.Mode {
	case types.ModeMemory:
		cfg = config.MemoryConfig()
		cfg.Port = *port
	case types.ModeDisk:
		cfg = config.DiskConfig()
		cfg.DataDir = *dataDir
		cfg.Port = *port
	case types.ModeColumnar:
		cfg = config.ColumnarConfig()
		cfg.Port = *port
	case types.ModeVector:
		cfg = config.VectorConfig(384) // Default to 384
		cfg.Port = *port
	}

	eng, err := kvi.Open(cfg)
	if err != nil {
		log.Fatalf("Failed to open KVi Engine: %v", err)
	}
	defer eng.Close()

	log.Printf("Starting Kvi (Kinetic Virtual Index) Engine v1.0.0 in %s mode", cfg.Mode)

	server := api.NewServer(eng)

	// Graceful shutdown handling
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Port)
		log.Printf("REST API listening on %s", addr)
		if err := server.Start(addr); err != nil {
			log.Fatalf("REST API server failed: %v", err)
		}
	}()

	<-stopChan
	log.Println("Shutting down Kvi engine gracefully...")
}
