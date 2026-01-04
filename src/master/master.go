package master

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ayanmali/mapreduce/src/chunker"
	"github.com/ayanmali/mapreduce/src/config"
	"github.com/ayanmali/mapreduce/src/rpc"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TaskStatus represents the state of a task
type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusAssigned
	TaskStatusInProgress
	TaskStatusCompleted
	TaskStatusFailed
)

// Master coordinates the MapReduce job
type Master struct {
	storageClient *minio.Client
	config        *config.MasterConfig
	bucket        string

	// Task tracking
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask
	taskLock    sync.RWMutex

	// Worker registry
	workers    map[string]*WorkerInfo
	workerLock sync.RWMutex

	// Phase coordination
	mapPhaseComplete    chan struct{}
	reducePhaseComplete chan struct{}
	jobComplete         chan struct{}

	// Job state
	jobID      string
	jobStarted bool
	jobLock    sync.Mutex
}

// MapTask represents a map task
type MapTask struct {
	TaskID            string
	Status            TaskStatus
	ChunkMetadata     chunker.ChunkMetadata
	AssignedWorker    string
	RetryCount        int
	LastHeartbeat     time.Time
	IntermediateFiles []*rpc.IntermediateFileInfo
}

// ReduceTask represents a reduce task
type ReduceTask struct {
	TaskID         string
	PartitionID    int
	Status         TaskStatus
	InputLocations []*rpc.IntermediateLocation
	AssignedWorker string
	RetryCount     int
	LastHeartbeat  time.Time
	OutputLocation string
}

// WorkerInfo contains information about a worker
type WorkerInfo struct {
	WorkerID      string
	Address       string
	LastHeartbeat time.Time
	ActiveTasks   []string
	Available     bool
}

// NewMaster creates a new Master instance
func NewMaster(storageClient *minio.Client, cfg *config.MasterConfig, bucket string) *Master {
	m := &Master{
		storageClient:       storageClient,
		config:              cfg,
		bucket:              bucket,
		mapTasks:            make([]*MapTask, 0),
		reduceTasks:         make([]*ReduceTask, 0),
		workers:             make(map[string]*WorkerInfo),
		mapPhaseComplete:    make(chan struct{}),
		reducePhaseComplete: make(chan struct{}),
		jobComplete:         make(chan struct{}),
		jobID:               uuid.New().String(),
	}

	// Register workers from config
	for _, addr := range cfg.WorkerAddresses {
		workerID := uuid.New().String()
		m.workers[workerID] = &WorkerInfo{
			WorkerID:      workerID,
			Address:       addr,
			LastHeartbeat: time.Now(),
			ActiveTasks:   make([]string, 0),
			Available:     true,
		}
	}

	return m
}

// SubmitJob starts a MapReduce job for the given input file
func (m *Master) SubmitJob(inputFile string) error {
	m.jobLock.Lock()
	if m.jobStarted {
		m.jobLock.Unlock()
		return fmt.Errorf("job already started")
	}
	m.jobStarted = true
	m.jobLock.Unlock()

	log.Printf("Starting job %s for input file: %s", m.jobID, inputFile)

	// Create line-aware chunks
	chunks, err := chunker.CreateLineAwareChunks(m.storageClient, m.bucket, inputFile, m.config.ChunkSizeBytes)
	if err != nil {
		return fmt.Errorf("failed to create chunks: %w", err)
	}

	log.Printf("Created %d chunks for file %s", len(chunks), inputFile)

	// Create map tasks
	m.taskLock.Lock()
	for i, chunk := range chunks {
		task := &MapTask{
			TaskID:        fmt.Sprintf("map-%s-%d", m.jobID, i),
			Status:        TaskStatusPending,
			ChunkMetadata: chunk,
			RetryCount:    0,
			LastHeartbeat: time.Now(),
		}
		m.mapTasks = append(m.mapTasks, task)
	}
	m.taskLock.Unlock()

	// Start task distribution and monitoring
	go m.distributeMapTasks()
	go m.monitorTaskProgress()

	return nil
}

// distributeMapTasks distributes pending map tasks to available workers
func (m *Master) distributeMapTasks() {
	for {
		m.taskLock.Lock()
		pendingTask := m.getNextPendingMapTask()
		m.taskLock.Unlock()

		if pendingTask == nil {
			// Check if all map tasks are completed
			if m.allMapTasksCompleted() {
				log.Println("All map tasks completed")
				close(m.mapPhaseComplete)
				go m.startReducePhase()
				return
			}

			// No pending tasks right now, wait a bit
			time.Sleep(1 * time.Second)
			continue
		}

		// Select an available worker
		m.workerLock.RLock()
		worker := m.selectAvailableWorker()
		m.workerLock.RUnlock()

		if worker == nil {
			// No workers available, wait
			time.Sleep(1 * time.Second)
			continue
		}

		// Mark task as assigned
		m.taskLock.Lock()
		pendingTask.Status = TaskStatusAssigned
		pendingTask.AssignedWorker = worker.WorkerID
		m.taskLock.Unlock()

		// Send task to worker in a goroutine
		go m.sendMapTaskToWorker(worker, pendingTask)
	}
}

// getNextPendingMapTask returns the next pending map task (must hold taskLock)
func (m *Master) getNextPendingMapTask() *MapTask {
	for _, task := range m.mapTasks {
		if task.Status == TaskStatusPending {
			return task
		}
	}
	return nil
}

// selectAvailableWorker returns an available worker (must hold workerLock for reading)
func (m *Master) selectAvailableWorker() *WorkerInfo {
	for _, worker := range m.workers {
		if worker.Available && len(worker.ActiveTasks) < 2 {
			return worker
		}
	}
	return nil
}

// sendMapTaskToWorker sends a map task to a worker via gRPC
func (m *Master) sendMapTaskToWorker(worker *WorkerInfo, task *MapTask) {
	log.Printf("Sending map task %s to worker %s", task.TaskID, worker.WorkerID)

	conn, err := grpc.NewClient(worker.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to worker %s: %v", worker.WorkerID, err)
		m.handleTaskFailure(task.TaskID)
		return
	}
	defer conn.Close()

	client := rpc.NewMapReduceClient(conn)

	request := &rpc.MapRequest{
		TaskId:         task.TaskID,
		Bucket:         task.ChunkMetadata.Bucket,
		ObjectKey:      task.ChunkMetadata.ObjectKey,
		ByteOffset:     task.ChunkMetadata.ByteOffset,
		ByteLength:     task.ChunkMetadata.ByteLength,
		NumReduceTasks: int32(m.config.NumReduceTasks),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := client.InvokeMapTask(ctx, request)
	if err != nil {
		log.Printf("RPC error for task %s: %v", task.TaskID, err)
		m.handleTaskFailure(task.TaskID)
		return
	}

	if result.Status == rpc.TaskStatus_FAILED {
		log.Printf("Task %s failed: %s", task.TaskID, result.ErrorMessage)
		m.handleTaskFailure(task.TaskID)
		return
	}

	// Task succeeded
	m.handleMapTaskCompletion(task.TaskID, result, worker)
}

// handleMapTaskCompletion handles successful completion of a map task
func (m *Master) handleMapTaskCompletion(taskID string, result *rpc.Result, worker *WorkerInfo) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	task := m.findMapTask(taskID)
	if task == nil {
		return
	}

	task.Status = TaskStatusCompleted
	task.IntermediateFiles = result.IntermediateFiles

	log.Printf("Map task %s completed successfully (%d records processed)", taskID, result.RecordsProcessed)

	// Update worker info
	m.workerLock.Lock()
	if w, exists := m.workers[worker.WorkerID]; exists {
		// Remove task from active tasks
		for i, t := range w.ActiveTasks {
			if t == taskID {
				w.ActiveTasks = append(w.ActiveTasks[:i], w.ActiveTasks[i+1:]...)
				break
			}
		}
	}
	m.workerLock.Unlock()
}

// handleTaskFailure handles task failure and retry logic
func (m *Master) handleTaskFailure(taskID string) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	task := m.findMapTask(taskID)
	if task == nil {
		// Try reduce task
		rtask := m.findReduceTask(taskID)
		if rtask == nil {
			return
		}

		rtask.RetryCount++
		if rtask.RetryCount < 3 {
			log.Printf("Reduce task %s failed, retrying (%d/3)", taskID, rtask.RetryCount)
			rtask.Status = TaskStatusPending
			rtask.AssignedWorker = ""
		} else {
			log.Printf("Reduce task %s failed permanently after 3 retries", taskID)
			rtask.Status = TaskStatusFailed
		}
		return
	}

	task.RetryCount++
	if task.RetryCount < 3 {
		log.Printf("Map task %s failed, retrying (%d/3)", taskID, task.RetryCount)
		task.Status = TaskStatusPending
		task.AssignedWorker = ""
	} else {
		log.Printf("Map task %s failed permanently after 3 retries", taskID)
		task.Status = TaskStatusFailed
	}
}

// findMapTask finds a map task by ID (must hold taskLock)
func (m *Master) findMapTask(taskID string) *MapTask {
	for _, task := range m.mapTasks {
		if task.TaskID == taskID {
			return task
		}
	}
	return nil
}

// findReduceTask finds a reduce task by ID (must hold taskLock)
func (m *Master) findReduceTask(taskID string) *ReduceTask {
	for _, task := range m.reduceTasks {
		if task.TaskID == taskID {
			return task
		}
	}
	return nil
}

// allMapTasksCompleted checks if all map tasks are completed
func (m *Master) allMapTasksCompleted() bool {
	m.taskLock.RLock()
	defer m.taskLock.RUnlock()

	for _, task := range m.mapTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}

// monitorTaskProgress monitors task progress and handles timeouts
func (m *Master) monitorTaskProgress() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.jobComplete:
			return
		case <-ticker.C:
			m.checkTaskTimeouts()
		}
	}
}

// checkTaskTimeouts checks for timed-out tasks and reassigns them
func (m *Master) checkTaskTimeouts() {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	now := time.Now()

	// Check map tasks
	for _, task := range m.mapTasks {
		if task.Status == TaskStatusInProgress && now.Sub(task.LastHeartbeat) > 30*time.Second {
			log.Printf("Map task %s timed out", task.TaskID)
			m.handleTaskFailure(task.TaskID)
		}
	}

	// Check reduce tasks
	for _, task := range m.reduceTasks {
		if task.Status == TaskStatusInProgress && now.Sub(task.LastHeartbeat) > 30*time.Second {
			log.Printf("Reduce task %s timed out", task.TaskID)
			m.handleTaskFailure(task.TaskID)
		}
	}
}

// startReducePhase starts the reduce phase after all map tasks are completed
func (m *Master) startReducePhase() {
	log.Println("Starting reduce phase")

	// Wait for map phase to complete
	<-m.mapPhaseComplete

	// Group intermediate files by partition ID
	partitionedFiles := make(map[int][]*rpc.IntermediateLocation)

	m.taskLock.RLock()
	for _, mapTask := range m.mapTasks {
		if mapTask.Status != TaskStatusCompleted {
			continue
		}

		worker := m.workers[mapTask.AssignedWorker]
		for _, fileInfo := range mapTask.IntermediateFiles {
			location := &rpc.IntermediateLocation{
				WorkerId:      mapTask.AssignedWorker,
				WorkerAddress: worker.Address,
				FilePath:      fileInfo.FilePath,
			}
			partitionedFiles[int(fileInfo.PartitionId)] = append(partitionedFiles[int(fileInfo.PartitionId)], location)
		}
	}
	m.taskLock.RUnlock()

	// Create reduce tasks
	m.taskLock.Lock()
	for partitionID, locations := range partitionedFiles {
		task := &ReduceTask{
			TaskID:         fmt.Sprintf("reduce-%s-%d", m.jobID, partitionID),
			PartitionID:    partitionID,
			Status:         TaskStatusPending,
			InputLocations: locations,
			RetryCount:     0,
			LastHeartbeat:  time.Now(),
		}
		m.reduceTasks = append(m.reduceTasks, task)
	}
	m.taskLock.Unlock()

	log.Printf("Created %d reduce tasks", len(m.reduceTasks))

	// Distribute reduce tasks
	go m.distributeReduceTasks()
}

// distributeReduceTasks distributes pending reduce tasks to available workers
func (m *Master) distributeReduceTasks() {
	for {
		m.taskLock.Lock()
		pendingTask := m.getNextPendingReduceTask()
		m.taskLock.Unlock()

		if pendingTask == nil {
			// Check if all reduce tasks are completed
			if m.allReduceTasksCompleted() {
				log.Println("All reduce tasks completed")
				close(m.reducePhaseComplete)
				close(m.jobComplete)
				return
			}

			// No pending tasks right now, wait a bit
			time.Sleep(1 * time.Second)
			continue
		}

		// Select an available worker
		m.workerLock.RLock()
		worker := m.selectAvailableWorker()
		m.workerLock.RUnlock()

		if worker == nil {
			// No workers available, wait
			time.Sleep(1 * time.Second)
			continue
		}

		// Mark task as assigned
		m.taskLock.Lock()
		pendingTask.Status = TaskStatusAssigned
		pendingTask.AssignedWorker = worker.WorkerID
		m.taskLock.Unlock()

		// Send task to worker in a goroutine
		go m.sendReduceTaskToWorker(worker, pendingTask)
	}
}

// getNextPendingReduceTask returns the next pending reduce task (must hold taskLock)
func (m *Master) getNextPendingReduceTask() *ReduceTask {
	for _, task := range m.reduceTasks {
		if task.Status == TaskStatusPending {
			return task
		}
	}
	return nil
}

// sendReduceTaskToWorker sends a reduce task to a worker via gRPC
func (m *Master) sendReduceTaskToWorker(worker *WorkerInfo, task *ReduceTask) {
	log.Printf("Sending reduce task %s to worker %s", task.TaskID, worker.WorkerID)

	conn, err := grpc.NewClient(worker.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to worker %s: %v", worker.WorkerID, err)
		m.handleTaskFailure(task.TaskID)
		return
	}
	defer conn.Close()

	client := rpc.NewMapReduceClient(conn)

	outputKey := fmt.Sprintf("output/reduce-%d.json", task.PartitionID)

	request := &rpc.ReduceRequest{
		TaskId:         task.TaskID,
		PartitionId:    int32(task.PartitionID),
		InputLocations: task.InputLocations,
		Bucket:         m.bucket,
		OutputKey:      outputKey,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := client.InvokeReduceTask(ctx, request)
	if err != nil {
		log.Printf("RPC error for task %s: %v", task.TaskID, err)
		m.handleTaskFailure(task.TaskID)
		return
	}

	if result.Status == rpc.TaskStatus_FAILED {
		log.Printf("Task %s failed: %s", task.TaskID, result.ErrorMessage)
		m.handleTaskFailure(task.TaskID)
		return
	}

	// Task succeeded
	m.handleReduceTaskCompletion(task.TaskID, outputKey, worker)
}

// handleReduceTaskCompletion handles successful completion of a reduce task
func (m *Master) handleReduceTaskCompletion(taskID, outputKey string, worker *WorkerInfo) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	task := m.findReduceTask(taskID)
	if task == nil {
		return
	}

	task.Status = TaskStatusCompleted
	task.OutputLocation = outputKey

	log.Printf("Reduce task %s completed successfully, output: %s", taskID, outputKey)

	// Update worker info
	m.workerLock.Lock()
	if w, exists := m.workers[worker.WorkerID]; exists {
		for i, t := range w.ActiveTasks {
			if t == taskID {
				w.ActiveTasks = append(w.ActiveTasks[:i], w.ActiveTasks[i+1:]...)
				break
			}
		}
	}
	m.workerLock.Unlock()
}

// allReduceTasksCompleted checks if all reduce tasks are completed
func (m *Master) allReduceTasksCompleted() bool {
	m.taskLock.RLock()
	defer m.taskLock.RUnlock()

	for _, task := range m.reduceTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}

// WaitForCompletion waits for the job to complete
func (m *Master) WaitForCompletion() {
	<-m.jobComplete
	log.Println("Job completed successfully")
}

// ReportTaskProgress handles progress reports from workers
func (m *Master) ReportTaskProgress(ctx context.Context, progress *rpc.TaskProgress) (*rpc.Ack, error) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	// Try to find map task
	task := m.findMapTask(progress.TaskId)
	if task != nil {
		task.LastHeartbeat = time.Now()
		if task.Status == TaskStatusAssigned {
			task.Status = TaskStatusInProgress
		}
		return &rpc.Ack{Acknowledged: true}, nil
	}

	// Try to find reduce task
	rtask := m.findReduceTask(progress.TaskId)
	if rtask != nil {
		rtask.LastHeartbeat = time.Now()
		if rtask.Status == TaskStatusAssigned {
			rtask.Status = TaskStatusInProgress
		}
		return &rpc.Ack{Acknowledged: true}, nil
	}

	return &rpc.Ack{Acknowledged: false}, nil
}
