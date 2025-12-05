package worker

import (
	"context"
	"log"
	"sync"

	"github.com/VoiculescuGeorge/sqs-consumer/internal/model"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/parser"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/service"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/sqsclient"
)

// MessageJob represents a message to be processed by a worker.
type MessageJob struct {
	RawMessage model.RawSQSMessage
	QueueName  string
}

// WorkerPool manages a pool of goroutines to process messages.
type WorkerPool struct {
	numWorkers int
	jobs       chan MessageJob
	results    chan error // For potential error reporting or monitoring
	wg         sync.WaitGroup
	parser     parser.MessageParser
	processor  service.MessageProcessor
	sqsClients map[string]*sqsclient.SQSClient
}

// NewWorkerPool creates and initializes a new WorkerPool.
func NewWorkerPool(
	numWorkers int,
	parser parser.MessageParser,
	processor service.MessageProcessor,
	clients []*sqsclient.SQSClient,
) *WorkerPool {
	clientMap := make(map[string]*sqsclient.SQSClient)
	for _, client := range clients {
		clientMap[client.GetQueueName()] = client
	}

	return &WorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan MessageJob, numWorkers*2), // Buffered channel
		results:    make(chan error, numWorkers),
		parser:     parser,
		processor:  processor,
		sqsClients: clientMap,
	}
}

// Start begins the worker goroutines.
func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i+1)
	}
	log.Printf("Worker pool started with %d workers.", wp.numWorkers)
}

// Stop waits for all workers to finish their current jobs.
func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
	close(wp.results)
	log.Println("Worker pool stopped.")
}

// SubmitJob adds a message to the job queue.
func (wp *WorkerPool) SubmitJob(job MessageJob) {
	wp.jobs <- job
}

// worker is the goroutine function that processes messages.
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()
	log.Printf("Worker %d started.", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down due to context cancellation.", id)
			return
		case job, ok := <-wp.jobs:
			if !ok {
				log.Printf("Worker %d shutting down, job channel closed.", id)
				return
			}
			wp.processMessage(ctx, job)
		}
	}
}

// processMessage handles the parsing, processing, and deletion of a single message.
func (wp *WorkerPool) processMessage(ctx context.Context, job MessageJob) {
	log.Printf("Worker processing message ID: %s from queue: %s", job.RawMessage.MessageID, job.QueueName)

	parsedMsg, err := wp.parser.Parse(job.RawMessage)
	if err != nil {
		log.Printf("Failed to parse message ID %s from queue %s: %v", job.RawMessage.MessageID, job.QueueName, err)
		return
	}

	err = wp.processor.Process(ctx, parsedMsg)
	if err != nil {
		log.Printf("Failed to process message ID %s from queue %s: %v", job.RawMessage.MessageID, job.QueueName, err)
		return
	}

	sqsClient, ok := wp.sqsClients[job.QueueName]
	if !ok {
		log.Printf("Error: SQS client not found for queue %s for message ID %s", job.QueueName, job.RawMessage.MessageID)
		return
	}

	err = sqsClient.DeleteMessage(ctx, job.RawMessage.ReceiptHandle)
	if err != nil {
		log.Printf("Failed to delete message ID %s from queue %s: %v", job.RawMessage.MessageID, job.QueueName, err)
	} else {
		log.Printf("Successfully processed and deleted message ID: %s from queue: %s", job.RawMessage.MessageID, job.QueueName)
	}
}
