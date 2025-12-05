package service

import (
	"context"
	"log"

	"github.com/VoiculescuGeorge/sqs-consumer/internal/model"
)

// MessageProcessor defines the interface for processing parsed messages.
type MessageProcessor interface {
	Process(ctx context.Context, msg *model.ParsedMessage) error
}

// Processor implements MessageProcessor.
type Processor struct {
	// Add any dependencies here, e.g., database client, HTTP client
}

// NewProcessor creates a new Processor.
func NewProcessor() *Processor {
	return &Processor{}
}

// Process handles the business logic for a parsed message.
func (p *Processor) Process(ctx context.Context, msg *model.ParsedMessage) error {
	log.Printf("Processing message ID: %s, Type: %s, Data: %+v", msg.ID, msg.Type, msg.Data)

	// Handle the message and do your logic
	return nil
}
