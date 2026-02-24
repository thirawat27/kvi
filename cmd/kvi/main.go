package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thirawat27/kvi/internal/pubsub"
	"github.com/thirawat27/kvi/pkg/api"
	"github.com/thirawat27/kvi/pkg/config"
	kvi_grpc "github.com/thirawat27/kvi/pkg/grpc"
	"github.com/thirawat27/kvi/pkg/kvi"
	"github.com/thirawat27/kvi/pkg/types"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[kvi] ")

	modeStr := flag.String("mode", string(types.ModeHybrid), "Engine mode: memory | disk | columnar | vector | hybrid")
	dataDir := flag.String("dir", "./data", "Data directory (for Disk / Hybrid modes)")
	port := flag.Int("port", 8080, "REST API port")
	grpcPort := flag.Int("grpc-port", 50051, "gRPC port")
	authOn := flag.Bool("auth", false, "Enable JWT authentication on all routes")
	cfgFile := flag.String("config", "", "Path to JSON config file (overrides flags)")
	flag.Parse()

	// ── Load config ──────────────────────────────────────────────────────────
	var cfg *config.Config

	if *cfgFile != "" {
		data, err := os.ReadFile(*cfgFile)
		if err != nil {
			log.Fatalf("Cannot read config file: %v", err)
		}
		cfg = config.DefaultConfig()
		if err := json.Unmarshal(data, cfg); err != nil {
			log.Fatalf("Invalid config JSON: %v", err)
		}
	} else {
		cfg = config.DefaultConfig()
		cfg.Mode = types.Mode(*modeStr)
		cfg.DataDir = *dataDir
		cfg.Port = *port
		cfg.GrpcPort = *grpcPort
	}

	// ── Open engine ──────────────────────────────────────────────────────────
	eng, err := kvi.Open(cfg)
	if err != nil {
		log.Fatalf("Failed to open engine: %v", err)
	}

	banner(cfg)

	// Shared pub/sub hub (REST + gRPC share it)
	hub := pubsub.NewHub()

	// ── REST API server ───────────────────────────────────────────────────────
	opts := []func(*api.Server){}
	if *authOn {
		log.Println("JWT authentication ENABLED")
		opts = append(opts, api.WithAuth())
	}
	restSrv := api.NewServer(eng, opts...)

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Port)
		log.Printf("REST API  → http://0.0.0.0%s", addr)
		if err := restSrv.Start(addr); err != nil {
			log.Fatalf("REST server error: %v", err)
		}
	}()

	// ── gRPC server ───────────────────────────────────────────────────────────
	go func() {
		addr := fmt.Sprintf(":%d", cfg.GrpcPort)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("gRPC listen error: %v", err)
		}
		gs := grpc.NewServer()
		kvi_grpc.RegisterKviServiceServer(gs, kvi_grpc.NewGrpcServer(eng, hub))
		log.Printf("gRPC API  → grpc://0.0.0.0%s", addr)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Kvi engine…")
	if err := eng.Close(); err != nil {
		log.Printf("Close error: %v", err)
	}
	log.Println("Goodbye 👋")
}

func banner(cfg *config.Config) {
	fmt.Println()
	fmt.Println("  ██╗  ██╗██╗   ██╗██╗")
	fmt.Println("  ██║ ██╔╝██║   ██║██║")
	fmt.Println("  █████╔╝ ██║   ██║██║")
	fmt.Println("  ██╔═██╗ ╚██╗ ██╔╝██║")
	fmt.Println("  ██║  ██╗ ╚████╔╝ ██║")
	fmt.Println("  ╚═╝  ╚═╝  ╚═══╝  ╚═╝")
	fmt.Printf("  Kinetic Virtual Index  v1.0.0\n\n")
	fmt.Printf("  Mode     : %s\n", cfg.Mode)
	fmt.Printf("  DataDir  : %s\n", cfg.DataDir)
	fmt.Printf("  REST     : http://0.0.0.0:%d\n", cfg.Port)
	fmt.Printf("  gRPC     : grpc://0.0.0.0:%d\n", cfg.GrpcPort)
	fmt.Printf("  Started  : %s\n\n", time.Now().Format(time.RFC3339))
}
