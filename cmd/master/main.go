package main

import (
	"flag"
	"log"

	"github.com/ayanmali/mapreduce/src/config"
	"github.com/ayanmali/mapreduce/src/master"
	"github.com/ayanmali/mapreduce/src/storage"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config-master.json", "Path to configuration file")
	inputFile := flag.String("input", "input/large-file.txt", "Input file in object storage")
	flag.Parse()

	log.Println("Starting MapReduce Master...")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration from %s", *configPath)
	log.Printf("Storage endpoint: %s", cfg.Storage.Endpoint)
	log.Printf("Master address: %s", cfg.Master.Address)
	log.Printf("Bucket: %s", cfg.Storage.BucketName)
	log.Printf("Chunk size: %d bytes", cfg.Master.ChunkSizeBytes)
	log.Printf("Reduce tasks: %d", cfg.Master.NumReduceTasks)
	log.Printf("Workers: %v", cfg.Master.WorkerAddresses)

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

	// Create Master
	m := master.NewMaster(storageClient, &cfg.Master, cfg.Storage.BucketName)

	log.Println("Master created successfully")

	// Start gRPC server in a goroutine
	go func() {
		if err := m.StartServer(); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	// Submit job
	log.Printf("Submitting MapReduce job for input: %s", *inputFile)
	if err := m.SubmitJob(*inputFile); err != nil {
		log.Fatalf("Failed to submit job: %v", err)
	}

	// Wait for completion
	log.Println("Waiting for job to complete...")
	m.WaitForCompletion()

	log.Println("MapReduce job completed successfully!")
}
