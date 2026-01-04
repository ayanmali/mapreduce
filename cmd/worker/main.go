package main

import (
	"flag"
	"log"

	"github.com/ayanmali/mapreduce/src/config"
	"github.com/ayanmali/mapreduce/src/storage"
	"github.com/ayanmali/mapreduce/src/worker"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config-worker.json", "Path to configuration file")
	workerID := flag.String("id", "", "Worker ID (optional, will use config if not provided)")
	flag.Parse()

	log.Println("Starting MapReduce Worker...")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Use command-line worker ID if provided, otherwise use config
	if *workerID != "" {
		cfg.Worker.WorkerID = *workerID
	}

	if cfg.Worker.WorkerID == "" {
		log.Fatal("Worker ID must be specified either in config or via -id flag")
	}

	log.Printf("Loaded configuration from %s", *configPath)
	log.Printf("Worker ID: %s", cfg.Worker.WorkerID)
	log.Printf("Storage endpoint: %s", cfg.Storage.Endpoint)
	log.Printf("Worker address: %s", cfg.Worker.Address)
	log.Printf("HTTP port: %s", cfg.Worker.HTTPPort)
	log.Printf("Master address: %s", cfg.Worker.MasterAddress)
	log.Printf("Work directory: %s", cfg.Worker.WorkDir)

	// Initialize storage client
	storageClient, err := storage.NewClient(storage.Config{
		Endpoint:        cfg.Storage.Endpoint,
		AccessKeyID:     cfg.Storage.AccessKeyID,
		SecretAccessKey: cfg.Storage.SecretAccessKey,
		UseSSL:          cfg.Storage.UseSSL,
		BucketName:      cfg.Storage.BucketName,
	})
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}

	log.Println("Connected to object storage")

	// Create Worker
	w, err := worker.NewWorker(cfg.Worker.WorkerID, storageClient, &cfg.Worker, cfg.Storage.BucketName)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	log.Printf("Worker %s created successfully", cfg.Worker.WorkerID)

	// Start gRPC server (blocks)
	log.Println("Starting gRPC server...")
	if err := w.StartServer(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
