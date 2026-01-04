package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ayanmali/mapreduce/src/config"
	"github.com/ayanmali/mapreduce/src/rpc"
	mapreduce "github.com/ayanmali/mapreduce/src"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Worker executes Map and Reduce tasks
type Worker struct {
	WorkerID       string
	MasterAddress  string
	ListenAddress  string
	HTTPAddress    string
	storageClient  *minio.Client
	bucket         string
	config         *config.WorkerConfig

	// Local storage
	workDir string

	// Task tracking
	activeTask     *TaskExecution
	taskLock       sync.Mutex
	progressTicker *time.Ticker
	stopProgress   chan struct{}
}

// TaskExecution tracks the currently executing task
type TaskExecution struct {
	TaskID           string
	StartTime        time.Time
	BytesProcessed   int64
	RecordsProcessed int64
}

// NewWorker creates a new Worker instance
func NewWorker(workerID string, storageClient *minio.Client, cfg *config.WorkerConfig, bucket string) (*Worker, error) {
	// Create work directory
	workDir := cfg.WorkDir
	if workDir == "" {
		workDir = filepath.Join(os.TempDir(), "mapreduce", workerID)
	}

	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work directory: %w", err)
	}

	w := &Worker{
		WorkerID:       workerID,
		MasterAddress:  cfg.MasterAddress,
		ListenAddress:  cfg.Address,
		HTTPAddress:    ":" + cfg.HTTPPort,
		storageClient:  storageClient,
		bucket:         bucket,
		config:         cfg,
		workDir:        workDir,
		stopProgress:   make(chan struct{}),
	}

	return w, nil
}

// InvokeMapTask executes a map task
func (w *Worker) InvokeMapTask(ctx context.Context, req *rpc.MapRequest) (*rpc.Result, error) {
	log.Printf("Worker %s received map task: %s", w.WorkerID, req.TaskId)

	// Start task execution tracking
	w.taskLock.Lock()
	w.activeTask = &TaskExecution{
		TaskID:    req.TaskId,
		StartTime: time.Now(),
	}
	w.taskLock.Unlock()

	// Start progress reporting
	go w.reportProgressPeriodically(req.TaskId)

	// 1. Fetch chunk from MinIO
	opts := minio.GetObjectOptions{}
	opts.SetRange(req.ByteOffset, req.ByteOffset+req.ByteLength-1)

	obj, err := w.storageClient.GetObject(ctx, req.Bucket, req.ObjectKey, opts)
	if err != nil {
		return &rpc.Result{
			TaskId:       req.TaskId,
			Status:       rpc.TaskStatus_FAILED,
			ErrorMessage: fmt.Sprintf("Failed to fetch chunk: %v", err),
		}, nil
	}

	chunkData, err := io.ReadAll(obj)
	obj.Close()
	if err != nil {
		return &rpc.Result{
			TaskId:       req.TaskId,
			Status:       rpc.TaskStatus_FAILED,
			ErrorMessage: fmt.Sprintf("Failed to read chunk: %v", err),
		}, nil
	}

	w.taskLock.Lock()
	if w.activeTask != nil {
		w.activeTask.BytesProcessed = int64(len(chunkData))
	}
	w.taskLock.Unlock()

	log.Printf("Fetched %d bytes for task %s", len(chunkData), req.TaskId)

	// 2. Execute Map function
	mapreduce.ClearIntermediateBuffer()
	mapreduce.Map(req.TaskId, string(chunkData))
	intermediateResults := mapreduce.GetIntermediateBuffer()

	log.Printf("Map produced %d intermediate pairs", len(intermediateResults))

	w.taskLock.Lock()
	if w.activeTask != nil {
		w.activeTask.RecordsProcessed = int64(len(intermediateResults))
	}
	w.taskLock.Unlock()

	// 3. Partition by key hash
	partitioned := w.partitionByKey(intermediateResults, int(req.NumReduceTasks))

	log.Printf("Partitioned into %d partitions", len(partitioned))

	// 4. Write each partition to local disk
	intermediateFiles := make([]*rpc.IntermediateFileInfo, 0)
	for partitionID, pairs := range partitioned {
		filename := fmt.Sprintf("map-%s-partition-%d.json", req.TaskId, partitionID)
		filePath := filepath.Join(w.workDir, filename)

		data, err := json.Marshal(pairs)
		if err != nil {
			return &rpc.Result{
				TaskId:       req.TaskId,
				Status:       rpc.TaskStatus_FAILED,
				ErrorMessage: fmt.Sprintf("Failed to serialize partition %d: %v", partitionID, err),
			}, nil
		}

		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return &rpc.Result{
				TaskId:       req.TaskId,
				Status:       rpc.TaskStatus_FAILED,
				ErrorMessage: fmt.Sprintf("Failed to write partition %d: %v", partitionID, err),
			}, nil
		}

		intermediateFiles = append(intermediateFiles, &rpc.IntermediateFileInfo{
			PartitionId: int32(partitionID),
			FilePath:    filename, // Store just the filename, not full path
		})

		log.Printf("Wrote partition %d (%d pairs) to %s", partitionID, len(pairs), filename)
	}

	// Stop progress reporting
	w.taskLock.Lock()
	w.activeTask = nil
	w.taskLock.Unlock()

	log.Printf("Map task %s completed successfully", req.TaskId)

	return &rpc.Result{
		TaskId:            req.TaskId,
		Status:            rpc.TaskStatus_SUCCESS,
		IntermediateFiles: intermediateFiles,
		RecordsProcessed:  int64(len(intermediateResults)),
	}, nil
}

// partitionByKey partitions intermediate results by key hash
func (w *Worker) partitionByKey(pairs []mapreduce.Pair, numPartitions int) map[int][]mapreduce.Pair {
	partitioned := make(map[int][]mapreduce.Pair)

	for _, pair := range pairs {
		key := fmt.Sprintf("%v", pair.First)
		partitionID := hashKey(key, numPartitions)

		partitioned[partitionID] = append(partitioned[partitionID], pair)
	}

	return partitioned
}

// hashKey computes a hash for a key and returns the partition ID
func hashKey(key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

// InvokeReduceTask executes a reduce task
func (w *Worker) InvokeReduceTask(ctx context.Context, req *rpc.ReduceRequest) (*rpc.Result, error) {
	log.Printf("Worker %s received reduce task: %s (partition %d)", w.WorkerID, req.TaskId, req.PartitionId)

	// Start task execution tracking
	w.taskLock.Lock()
	w.activeTask = &TaskExecution{
		TaskID:    req.TaskId,
		StartTime: time.Now(),
	}
	w.taskLock.Unlock()

	// Start progress reporting
	go w.reportProgressPeriodically(req.TaskId)

	// 1. Fetch intermediate files from map workers via HTTP
	allPairs := []mapreduce.Pair{}

	for _, location := range req.InputLocations {
		// HTTP GET from worker
		url := fmt.Sprintf("http://%s/intermediate/%s", location.WorkerAddress, filepath.Base(location.FilePath))

		log.Printf("Fetching intermediate file from %s", url)

		resp, err := http.Get(url)
		if err != nil {
			return &rpc.Result{
				TaskId:       req.TaskId,
				Status:       rpc.TaskStatus_FAILED,
				ErrorMessage: fmt.Sprintf("Failed to fetch from %s: %v", url, err),
			}, nil
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return &rpc.Result{
				TaskId:       req.TaskId,
				Status:       rpc.TaskStatus_FAILED,
				ErrorMessage: fmt.Sprintf("Failed to read response from %s: %v", url, err),
			}, nil
		}

		var pairs []mapreduce.Pair
		if err := json.Unmarshal(data, &pairs); err != nil {
			return &rpc.Result{
				TaskId:       req.TaskId,
				Status:       rpc.TaskStatus_FAILED,
				ErrorMessage: fmt.Sprintf("Failed to parse intermediate file: %v", err),
			}, nil
		}

		allPairs = append(allPairs, pairs...)

		log.Printf("Fetched %d pairs from %s", len(pairs), url)
	}

	log.Printf("Total pairs for reduce: %d", len(allPairs))

	// 2. Group by key
	grouped := groupByKey(allPairs)

	log.Printf("Grouped into %d unique keys", len(grouped))

	// 3. Execute Reduce function for each key
	results := make(map[string]interface{})
	for key, values := range grouped {
		result := mapreduce.ReduceToValue(key, values)
		results[fmt.Sprintf("%v", key)] = result
	}

	log.Printf("Reduce produced %d results", len(results))

	w.taskLock.Lock()
	if w.activeTask != nil {
		w.activeTask.RecordsProcessed = int64(len(results))
	}
	w.taskLock.Unlock()

	// 4. Upload final results to MinIO
	resultData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return &rpc.Result{
			TaskId:       req.TaskId,
			Status:       rpc.TaskStatus_FAILED,
			ErrorMessage: fmt.Sprintf("Failed to serialize results: %v", err),
		}, nil
	}

	reader := bytes.NewReader(resultData)
	_, err = w.storageClient.PutObject(
		ctx,
		req.Bucket,
		req.OutputKey,
		reader,
		int64(len(resultData)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)

	if err != nil {
		return &rpc.Result{
			TaskId:       req.TaskId,
			Status:       rpc.TaskStatus_FAILED,
			ErrorMessage: fmt.Sprintf("Failed to upload results: %v", err),
		}, nil
	}

	// Stop progress reporting
	w.taskLock.Lock()
	w.activeTask = nil
	w.taskLock.Unlock()

	log.Printf("Reduce task %s completed successfully, uploaded to %s", req.TaskId, req.OutputKey)

	return &rpc.Result{
		TaskId:           req.TaskId,
		Status:           rpc.TaskStatus_SUCCESS,
		RecordsProcessed: int64(len(results)),
	}, nil
}

// groupByKey groups pairs by their key
func groupByKey(pairs []mapreduce.Pair) map[interface{}][]interface{} {
	grouped := make(map[interface{}][]interface{})
	for _, pair := range pairs {
		grouped[pair.First] = append(grouped[pair.First], pair.Second)
	}
	return grouped
}

// reportProgressPeriodically reports progress to the Master periodically
func (w *Worker) reportProgressPeriodically(taskID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	conn, err := grpc.NewClient(w.MasterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to master for progress reporting: %v", err)
		return
	}
	defer conn.Close()

	client := rpc.NewMapReduceClient(conn)

	for {
		select {
		case <-ticker.C:
			w.taskLock.Lock()
			if w.activeTask == nil || w.activeTask.TaskID != taskID {
				w.taskLock.Unlock()
				return
			}

			progress := &rpc.TaskProgress{
				TaskId:         taskID,
				WorkerId:       w.WorkerID,
				BytesProcessed: w.activeTask.BytesProcessed,
			}
			w.taskLock.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.ReportTaskProgress(ctx, progress)
			cancel()

			if err != nil {
				log.Printf("Failed to report progress: %v", err)
			}

		case <-w.stopProgress:
			return
		}
	}
}

// StartFileServer starts an HTTP server to serve intermediate files
func (w *Worker) StartFileServer() error {
	http.HandleFunc("/intermediate/", func(rw http.ResponseWriter, r *http.Request) {
		filename := filepath.Base(r.URL.Path)
		filePath := filepath.Join(w.workDir, filename)

		log.Printf("Serving intermediate file: %s", filename)

		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			http.Error(rw, "File not found", http.StatusNotFound)
			return
		}

		http.ServeFile(rw, r, filePath)
	})

	log.Printf("Worker %s HTTP file server listening on %s", w.WorkerID, w.HTTPAddress)

	return http.ListenAndServe(w.HTTPAddress, nil)
}
