package config

import (
	"encoding/json"
	"os"
)

// MapReduceConfig contains all configuration for the MapReduce system
type MapReduceConfig struct {
	Storage StorageConfig `json:"storage"`
	Master  MasterConfig  `json:"master"`
	Worker  WorkerConfig  `json:"worker"`
}

// StorageConfig contains object storage configuration
type StorageConfig struct {
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	UseSSL          bool   `json:"use_ssl"`
	BucketName      string `json:"bucket_name"`
}

// MasterConfig contains Master node configuration
type MasterConfig struct {
	Address        string   `json:"address"`
	ChunkSizeBytes int64    `json:"chunk_size_bytes"`
	NumReduceTasks int      `json:"num_reduce_tasks"`
	WorkerAddresses []string `json:"worker_addresses"`
}

// WorkerConfig contains Worker node configuration
type WorkerConfig struct {
	WorkerID      string `json:"worker_id"`
	Address       string `json:"address"`
	MasterAddress string `json:"master_address"`
	WorkDir       string `json:"work_dir"`
	HTTPPort      string `json:"http_port"`
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(path string) (*MapReduceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg MapReduceConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// SaveConfig saves configuration to a JSON file
func SaveConfig(path string, cfg *MapReduceConfig) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}
