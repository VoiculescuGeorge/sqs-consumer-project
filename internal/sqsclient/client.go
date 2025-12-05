package sqsclient

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/VoiculescuGeorge/sqs-consumer/internal/config"
	"github.com/VoiculescuGeorge/sqs-consumer/internal/model"
)

// SQSClient manages interactions with a specific SQS queue.
type SQSClient struct {
	client *sqs.Client
	queue  config.QueueConfig
}

// NewSQSClients initializes SQS clients for all configured queues.
func NewSQSClients(ctx context.Context, appConfig *config.AppConfig) ([]*SQSClient, error) {
	var clients []*SQSClient
	for _, qCfg := range appConfig.Queues {
		cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(qCfg.Region))
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config for region %s: %w", qCfg.Region, err)
		}
		clients = append(clients, &SQSClient{
			client: sqs.NewFromConfig(cfg),
			queue:  qCfg,
		})
		log.Printf("Initialized SQS client for queue: %s in region: %s", qCfg.Name, qCfg.Region)
	}
	return clients, nil
}

// ReceiveMessages polls the SQS queue for messages.
func (s *SQSClient) ReceiveMessages(ctx context.Context) ([]model.RawSQSMessage, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.queue.URL),
		MaxNumberOfMessages: s.queue.MaxMessages,
		WaitTimeSeconds:     s.queue.WaitTime,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameAll, // Or specific attributes you need
		},
	}

	output, err := s.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to receive messages from queue %s: %w", s.queue.Name, err)
	}

	var rawMessages []model.RawSQSMessage
	for _, msg := range output.Messages {
		rawMessages = append(rawMessages, model.RawSQSMessage{
			MessageID:     aws.ToString(msg.MessageId),
			ReceiptHandle: aws.ToString(msg.ReceiptHandle),
			Body:          aws.ToString(msg.Body),
			Attributes:    msg.Attributes,
		})
	}
	return rawMessages, nil
}

// DeleteMessage deletes a message from the SQS queue.
func (s *SQSClient) DeleteMessage(ctx context.Context, receiptHandle string) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queue.URL),
		ReceiptHandle: aws.String(receiptHandle),
	}
	_, err := s.client.DeleteMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete message with receipt handle %s from queue %s: %w", receiptHandle, s.queue.Name, err)
	}
	return nil
}

// GetQueueName returns the name of the queue this client is associated with.
func (s *SQSClient) GetQueueName() string {
	return s.queue.Name
}
