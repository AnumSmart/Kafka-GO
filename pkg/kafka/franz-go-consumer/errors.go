package franzgoconsumer

import "errors"

var (
	// Ошибки инициализации
	ErrClientNotInitialized  = errors.New("kafka client is not initialized")
	ErrHandlerNotInitialized = errors.New("message handler is not initialized")

	// Ошибки работы consumer
	ErrConsumerAlreadyRunning = errors.New("consumer is already running")
	ErrConsumerStopped        = errors.New("consumer is stopped")
	ErrCommitFailed           = errors.New("failed to commit offsets")

	// Ошибки DLQ
	ErrDLQBufferFull = errors.New("DLQ buffer is full, message dropped")
	ErrDLQNotEnabled = errors.New("DLQ is not enabled")

	// Ошибки конфигурации
	ErrInvalidConfig = errors.New("invalid consumer configuration")
	ErrInvalidTopics = errors.New("no topics to consume")
)
