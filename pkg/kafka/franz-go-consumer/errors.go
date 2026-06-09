package franzgoconsumer

import "errors"

var (
	// ErrClientNotInitialized - Kafka клиент не инициализирован
	ErrClientNotInitialized = errors.New("kafka client is not initialized")

	// ErrHandlerNotInitialized - обработчик сообщений не инициализирован
	ErrHandlerNotInitialized = errors.New("message handler is not initialized")
)
