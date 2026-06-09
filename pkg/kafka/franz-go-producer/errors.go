package franzgoproducer

import "errors"

var (
	// ErrProducerNotInitialized - продьюсер не инициализирован
	ErrProducerNotInitialized = errors.New("producer is not initialized")

	// ErrSendFailed - ошибка отправки сообщения
	ErrSendFailed = errors.New("failed to send message")

	// ErrEmptyMessage - пустое сообщение
	ErrEmptyMessage = errors.New("message is nil")

	// ErrTopicNotSpecified - топик не указан
	ErrTopicNotSpecified = errors.New("topic is not specified")

	// ErrEmptyBatch - пустой батч
	ErrEmptyBatch = errors.New("batch is empty")

	// Validation errors
	ErrInvalidRequiredAcks   = errors.New("required_acks must be -1, 0, or 1")
	ErrInvalidMaxRetries     = errors.New("max_retries must be >= 0")
	ErrInvalidRetryBackoff   = errors.New("retry_backoff must be > 0")
	ErrInvalidBatchBytes     = errors.New("batch_bytes must be > 0")
	ErrInvalidBatchTimeout   = errors.New("batch_timeout must be > 0")
	ErrInvalidTimeout        = errors.New("timeout must be > 0")
	ErrInvalidRequestTimeout = errors.New("request_timeout must be > 0")
	ErrInvalidMaxRecordSize  = errors.New("max_record_size_bytes must be > 0")
)
