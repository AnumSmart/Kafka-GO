package kafka

import "time"

// Message - универсальное сообщение Kafka (абстракция от конкретной библиотеки)
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   map[string][]byte

	// Для дополнительных метаданных (опционально)
	Metadata map[string]interface{}
}

// DLQMessage - структура для сериализации в DLQ
type DLQMessage struct {
	Metadata DLQMetadata `json:"metadata"`
	Payload  DLQPayload  `json:"payload"`
}

type DLQMetadata struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int32     `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	Timestamp         time.Time `json:"timestamp"`
	Error             string    `json:"error"`
	Service           string    `json:"service"`
	RetryCount        int       `json:"retry_count,omitempty"`
}

type DLQPayload struct {
	Key     string            `json:"key,omitempty"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}
