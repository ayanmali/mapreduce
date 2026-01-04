# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a distributed MapReduce implementation in Go using gRPC for inter-process communication. The system follows the classic MapReduce paradigm with a Master-Worker architecture.

## Architecture

**Core Components:**

- `src/main.go` - Contains the MapReduce framework implementation with Map/Reduce functions, intermediate buffer management, and key grouping logic
- `src/master.go` - Master node that coordinates MapReduce jobs by sending RPC requests to workers
- `src/worker.go` - Worker nodes that receive and execute Map/Reduce tasks via gRPC
- `src/load_balancer.go` - Load balancing logic (placeholder)
- `src/rpc/` - gRPC service definitions and generated code

**Communication Flow:**

1. Master sends Map/Reduce task requests to Workers via gRPC (InvokeMapTask/InvokeReduceTask RPCs)
2. Workers execute tasks and return results
3. The framework uses an intermediate buffer to store Map output before grouping by key
4. Grouped intermediate values are passed to Reduce functions

**Key Design Patterns:**

- Map phase: Processes input key-value pairs and emits intermediate pairs via `MapEmit()`
- Grouping phase: `groupByKey()` collects all values for each intermediate key
- Reduce phase: Processes grouped values and emits final results via `ReduceEmit()`

## Development Commands

**Build:**
```bash
go build -o main src/main.go
```

**Run:**
```bash
./main
# or
go run src/main.go
```

**Install dependencies:**
```bash
go mod download
```

**Regenerate gRPC code from proto file:**
```bash
protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  src/rpc/service.proto
```

## Code Structure Notes

- The main package is `mapreduce` not `main` in most files
- gRPC service is defined in `src/rpc/service.proto` with two RPC methods: InvokeMapTask and InvokeReduceTask
- The current implementation uses a simple in-memory intermediate buffer (`intermediateBuffer`) for Map output
- Master and Worker components are currently stubs with TODO implementations for RPC logic
