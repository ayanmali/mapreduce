package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config contains configuration for object storage
type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
}

// NewClient creates a new MinIO client and ensures the bucket exists
func NewClient(cfg Config) (*minio.Client, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	// Ensure bucket exists
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, cfg.BucketName)
	if err != nil {
		return nil, err
	}

	if !exists {
		err = client.MakeBucket(ctx, cfg.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

// GetFileMetadata returns metadata for a file in object storage
func GetFileMetadata(client *minio.Client, bucket, key string) (minio.ObjectInfo, error) {
	ctx := context.Background()
	return client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
}

// ReadChunk reads a specific byte range from an object
func ReadChunk(client *minio.Client, bucket, key string, offset, length int64) ([]byte, error) {
	ctx := context.Background()
	opts := minio.GetObjectOptions{}
	opts.SetRange(offset, offset+length-1)

	obj, err := client.GetObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	return io.ReadAll(obj)
}

// ListFiles lists all files with a given prefix in the bucket
func ListFiles(client *minio.Client, bucket, prefix string) ([]string, error) {
	ctx := context.Background()
	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	var files []string
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		files = append(files, object.Key)
	}

	return files, nil
}

// UploadFile uploads data to object storage
func UploadFile(client *minio.Client, bucket, key string, data []byte) error {
	ctx := context.Background()
	reader := bytes.NewReader(data)

	_, err := client.PutObject(
		ctx,
		bucket,
		key,
		reader,
		int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)

	return err
}
