package model

import "time"

// RawSQSMessage represents the relevant parts of an AWS SQS message.
type RawSQSMessage struct {
	MessageID     string
	ReceiptHandle string
	Body          string
	Attributes    map[string]string
}

// ParsedMessage represents the structured data after parsing the SQS message body.
type ParsedMessage struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}
