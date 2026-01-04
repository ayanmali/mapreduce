package worker

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/ayanmali/mapreduce/src/rpc"
	"google.golang.org/grpc"
)

// ServerImplementation wraps Worker to implement gRPC server interface
type ServerImplementation struct {
	rpc.UnimplementedMapReduceServer
	worker *Worker
}

// NewServerImplementation creates a new server implementation
func NewServerImplementation(worker *Worker) *ServerImplementation {
	return &ServerImplementation{
		worker: worker,
	}
}

// InvokeMapTask implements the gRPC InvokeMapTask method
func (s *ServerImplementation) InvokeMapTask(ctx context.Context, req *rpc.MapRequest) (*rpc.Result, error) {
	return s.worker.InvokeMapTask(ctx, req)
}

// InvokeReduceTask implements the gRPC InvokeReduceTask method
func (s *ServerImplementation) InvokeReduceTask(ctx context.Context, req *rpc.ReduceRequest) (*rpc.Result, error) {
	return s.worker.InvokeReduceTask(ctx, req)
}

// ReportTaskProgress implements the gRPC ReportTaskProgress method (not used by Worker)
func (s *ServerImplementation) ReportTaskProgress(ctx context.Context, progress *rpc.TaskProgress) (*rpc.Ack, error) {
	return nil, fmt.Errorf("ReportTaskProgress not implemented on Worker")
}

// GetIntermediateData implements the gRPC GetIntermediateData method (not used - using HTTP instead)
func (s *ServerImplementation) GetIntermediateData(req *rpc.IntermediateDataRequest, stream rpc.MapReduce_GetIntermediateDataServer) error {
	return fmt.Errorf("GetIntermediateData not implemented - use HTTP endpoint instead")
}

// StartServer starts the gRPC server for the Worker
func (w *Worker) StartServer() error {
	lis, err := net.Listen("tcp", w.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.ListenAddress, err)
	}

	grpcServer := grpc.NewServer()
	serverImpl := NewServerImplementation(w)
	rpc.RegisterMapReduceServer(grpcServer, serverImpl)

	log.Printf("Worker %s gRPC server listening on %s", w.WorkerID, w.ListenAddress)

	// Start HTTP file server in a separate goroutine
	go func() {
		if err := w.StartFileServer(); err != nil {
			log.Fatalf("HTTP file server failed: %v", err)
		}
	}()

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
