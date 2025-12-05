package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/VoiculescuGeorge/sqs-consumer/internal/config"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/parser"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/service"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/sqsclient"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/worker"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appConfig, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	log.Printf("Configuration loaded. Worker pool size: %d", appConfig.WorkerPoolSize)

	sqsClients, err := sqsclient.NewSQSClients(ctx, appConfig)
	if err != nil {
		log.Fatalf("Failed to initialize SQS clients: %v", err)
	}
	if len(sqsClients) == 0 {
		log.Fatalf("No SQS queues configured to listen to.")
	}

	msgParser := parser.NewJSONParser()
	msgProcessor := service.NewProcessor()

	workerPool := worker.NewWorkerPool(appConfig.WorkerPoolSize, msgParser, msgProcessor, sqsClients)
	workerPool.Start(ctx)

	var wg sync.WaitGroup
	for _, client := range sqsClients {
		wg.Add(1)
		go func(c *sqsclient.SQSClient) {
			defer wg.Done()
			pollSQSQueue(ctx, c, workerPool)
		}(client)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Println("Shutdown signal received. Initiating graceful shutdown...")
		cancel()
	case <-ctx.Done():
		log.Println("Context cancelled, initiating graceful shutdown...")
	}

	wg.Wait()

	workerPool.Stop()

	log.Println("Application gracefully shut down.")
}

func pollSQSQueue(ctx context.Context, client *sqsclient.SQSClient, wp *worker.WorkerPool) {
	queueName := client.GetQueueName()
	log.Printf("Starting SQS poller for queue: %s", queueName)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping SQS poller for queue: %s", queueName)
			return
		default:
			messages, err := client.ReceiveMessages(ctx)
			if err != nil {
				log.Printf("Error receiving messages from queue %s: %v", queueName, err)
				time.Sleep(5 * time.Second)
				continue
			}

			if len(messages) == 0 {
				log.Printf("No messages received from queue %s. Waiting...", queueName)

				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf("Received %d messages from queue %s. Submitting to worker pool.", len(messages), queueName)
			for _, msg := range messages {
				wp.SubmitJob(worker.MessageJob{
					RawMessage: msg,
					QueueName:  queueName,
				})
			}
		}
	}
}
