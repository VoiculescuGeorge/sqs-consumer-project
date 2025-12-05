package parser

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/VoiculescuGeorge/sqs-consumer/internal/model"
)

// MessageParser defines an interface for parsing raw SQS messages.
type MessageParser interface {
	Parse(rawMsg model.RawSQSMessage) (*model.ParsedMessage, error)
}

// JSONParser implements MessageParser for JSON message bodies.
type JSONParser struct{}

// NewJSONParser creates a new JSONParser.
func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

// Parse attempts to unmarshal the SQS message body as JSON into a ParsedMessage.
func (p *JSONParser) Parse(rawMsg model.RawSQSMessage) (*model.ParsedMessage, error) {
	var parsedMsg model.ParsedMessage
	if err := json.Unmarshal([]byte(rawMsg.Body), &parsedMsg); err != nil {
		log.Printf("Error unmarshalling message ID %s: %v", rawMsg.MessageID, err)
		return nil, fmt.Errorf("failed to parse message body: %w", err)
	}
	return &parsedMsg, nil
}
