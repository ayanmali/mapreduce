package chunker

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
)

// ChunkMetadata contains information about a chunk of a file
type ChunkMetadata struct {
	Bucket     string
	ObjectKey  string
	ByteOffset int64
	ByteLength int64
}

// CreateLineAwareChunks splits a file into chunks that respect line boundaries
// targetSize is the target chunk size in bytes (e.g., 10MB)
// The actual chunk sizes may vary to ensure complete lines
func CreateLineAwareChunks(client *minio.Client, bucket, key string, targetSize int64) ([]ChunkMetadata, error) {
	ctx := context.Background()

	// Get file size
	objInfo, err := client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object metadata: %w", err)
	}

	fileSize := objInfo.Size

	// If file is smaller than target size, return single chunk
	if fileSize <= targetSize {
		return []ChunkMetadata{
			{
				Bucket:     bucket,
				ObjectKey:  key,
				ByteOffset: 0,
				ByteLength: fileSize,
			},
		}, nil
	}

	chunks := []ChunkMetadata{}
	currentOffset := int64(0)

	for currentOffset < fileSize {
		// Calculate tentative chunk end
		// Read a bit extra (1KB) to find the line boundary
		bufferSize := int64(1024)
		chunkEnd := currentOffset + targetSize

		if chunkEnd >= fileSize {
			// Last chunk - take everything remaining
			chunks = append(chunks, ChunkMetadata{
				Bucket:     bucket,
				ObjectKey:  key,
				ByteOffset: currentOffset,
				ByteLength: fileSize - currentOffset,
			})
			break
		}

		// Read from chunkEnd to chunkEnd + bufferSize to find newline
		readEnd := min(chunkEnd+bufferSize, fileSize)

		opts := minio.GetObjectOptions{}
		opts.SetRange(chunkEnd, readEnd-1)

		obj, err := client.GetObject(ctx, bucket, key, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk boundary at offset %d: %w", chunkEnd, err)
		}

		data, err := io.ReadAll(obj)
		obj.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk boundary data: %w", err)
		}

		// Find the first newline in the buffer
		newlineIndex := bytes.IndexByte(data, '\n')

		var actualChunkLength int64
		if newlineIndex == -1 {
			// No newline found in buffer - this might be a very long line
			// In this case, we'll split at the target size (not ideal but necessary)
			actualChunkLength = targetSize
		} else {
			// Include the newline character in this chunk
			actualChunkLength = targetSize + int64(newlineIndex) + 1
		}

		chunks = append(chunks, ChunkMetadata{
			Bucket:     bucket,
			ObjectKey:  key,
			ByteOffset: currentOffset,
			ByteLength: actualChunkLength,
		})

		currentOffset += actualChunkLength
	}

	return chunks, nil
}

// CreateFixedSizeChunks splits a file into fixed-size chunks (simpler, no line awareness)
func CreateFixedSizeChunks(client *minio.Client, bucket, key string, chunkSize int64) ([]ChunkMetadata, error) {
	ctx := context.Background()

	// Get file size
	objInfo, err := client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object metadata: %w", err)
	}

	fileSize := objInfo.Size
	numChunks := (fileSize + chunkSize - 1) / chunkSize

	chunks := make([]ChunkMetadata, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		offset := i * chunkSize
		length := chunkSize
		if offset+length > fileSize {
			length = fileSize - offset
		}

		chunks = append(chunks, ChunkMetadata{
			Bucket:     bucket,
			ObjectKey:  key,
			ByteOffset: offset,
			ByteLength: length,
		})
	}

	return chunks, nil
}

// min returns the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
