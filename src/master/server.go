package master

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/ayanmali/mapreduce/src/rpc"
	"google.golang.org/grpc"
)

// ServerImplementation wraps Master to implement gRPC server interface
type ServerImplementation struct {
	rpc.UnimplementedMapReduceServer
	master *Master
}

// NewServerImplementation creates a new server implementation
func NewServerImplementation(master *Master) *ServerImplementation {
	return &ServerImplementation{
		master: master,
	}
}

// ReportTaskProgress implements the gRPC ReportTaskProgress method
func (s *ServerImplementation) ReportTaskProgress(ctx context.Context, progress *rpc.TaskProgress) (*rpc.Ack, error) {
	return s.master.ReportTaskProgress(ctx, progress)
}

// InvokeMapTask implements the gRPC InvokeMapTask method (not used by Master)
func (s *ServerImplementation) InvokeMapTask(ctx context.Context, req *rpc.MapRequest) (*rpc.Result, error) {
	return nil, fmt.Errorf("InvokeMapTask not implemented on Master")
}

// InvokeReduceTask implements the gRPC InvokeReduceTask method (not used by Master)
func (s *ServerImplementation) InvokeReduceTask(ctx context.Context, req *rpc.ReduceRequest) (*rpc.Result, error) {
	return nil, fmt.Errorf("InvokeReduceTask not implemented on Master")
}

// GetIntermediateData implements the gRPC GetIntermediateData method (not used by Master)
func (s *ServerImplementation) GetIntermediateData(req *rpc.IntermediateDataRequest, stream rpc.MapReduce_GetIntermediateDataServer) error {
	return fmt.Errorf("GetIntermediateData not implemented on Master")
}

// StartServer starts the gRPC server for the Master
func (m *Master) StartServer() error {
	lis, err := net.Listen("tcp", m.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", m.config.Address, err)
	}

	grpcServer := grpc.NewServer()
	serverImpl := NewServerImplementation(m)
	rpc.RegisterMapReduceServer(grpcServer, serverImpl)

	log.Printf("Master gRPC server listening on %s", m.config.Address)

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
