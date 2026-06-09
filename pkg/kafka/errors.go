package kafka

import "errors"

var (
	// Consumer errors
	ErrConsumerStopped = errors.New("consumer is stopped")

	// Producer errors
	ErrProducerNotReady = errors.New("producer is not ready")
	ErrSendFailed       = errors.New("failed to send message")

	// DLQ errors
	ErrDLQNotEnabled = errors.New("DLQ is not enabled")
	ErrDLQSendFailed = errors.New("failed to send to DLQ")

	// Message errors
	ErrMessageTooLarge = errors.New("message too large")
	ErrInvalidMessage  = errors.New("invalid message")
)
