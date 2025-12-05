package config

import (
	"fmt"
	"os"
	"strconv"
)

type QueueConfig struct {
	Name        string
	URL         string
	Region      string
	MaxMessages int32
	WaitTime    int32
}

type AppConfig struct {
	Queues         []QueueConfig
	WorkerPoolSize int
}

// LoadConfig loads configuration from environment variables.
// TODO: Add viper config managment
func LoadConfig() (*AppConfig, error) {
	queueURL := os.Getenv("SQS_QUEUE_URL")
	queueRegion := os.Getenv("SQS_QUEUE_REGION")
	queueName := os.Getenv("SQS_QUEUE_NAME")
	workerPoolSizeStr := os.Getenv("WORKER_POOL_SIZE")

	if queueURL == "" || queueRegion == "" || queueName == "" {
		return nil, fmt.Errorf("SQS_QUEUE_URL, SQS_QUEUE_REGION, and SQS_QUEUE_NAME must be set")
	}

	workerPoolSize, err := strconv.Atoi(workerPoolSizeStr)
	if err != nil || workerPoolSize <= 0 {
		workerPoolSize = 5
	}

	return &AppConfig{
		Queues: []QueueConfig{
			{
				Name:        queueName,
				URL:         queueURL,
				Region:      queueRegion,
				MaxMessages: 10,
				WaitTime:    20,
			},
		},
		WorkerPoolSize: workerPoolSize,
	}, nil
}
