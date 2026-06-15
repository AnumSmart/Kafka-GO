package franzgoconsumer

import (
	"pkg/kafka"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ============================================================================
// Тест: ToKafkaMessage - конвертация kgo.Record → kafka.Message
// ============================================================================

func TestToKafkaMessage(t *testing.T) {
	// Подробное объяснение в тексте после кода
	timestamp := time.Now().Truncate(time.Millisecond)

	// Создаем тестовый kgo.Record
	record := &kgo.Record{
		Topic:     "test-topic",
		Partition: 42,
		Offset:    12345,
		Timestamp: timestamp,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Headers: []kgo.RecordHeader{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "trace-id", Value: []byte("abc-123")},
		},
	}

	// Вызываем тестируемую функцию
	result := ToKafkaMessage(record)

	// Проверяем результат
	require.NotNil(t, result, "Result should not be nil")

	assert.Equal(t, "test-topic", result.Topic)
	assert.Equal(t, int32(42), result.Partition)
	assert.Equal(t, int64(12345), result.Offset)
	assert.Equal(t, timestamp, result.Timestamp)
	assert.Equal(t, []byte("test-key"), result.Key)
	assert.Equal(t, []byte("test-value"), result.Value)

	// Проверяем заголовки (мапа)
	expectedHeaders := map[string][]byte{
		"content-type": []byte("application/json"),
		"trace-id":     []byte("abc-123"),
	}
	assert.Equal(t, expectedHeaders, result.Headers)

	// Проверяем, что Metadata инициализирован (не nil)
	assert.NotNil(t, result.Metadata, "Metadata map should be initialized")
}

// ============================================================================
// Тест: ToKafkaMessage с nil входным параметром
// ============================================================================

func TestToKafkaMessage_NilInput(t *testing.T) {
	result := ToKafkaMessage(nil)
	assert.Nil(t, result, "Should return nil when input is nil")
}

// ============================================================================
// Тест: ToKafkaMessage с пустыми заголовками
// ============================================================================

func TestToKafkaMessage_EmptyHeaders(t *testing.T) {
	record := &kgo.Record{
		Topic:   "test-topic",
		Headers: []kgo.RecordHeader{}, // пустые заголовки
	}

	result := ToKafkaMessage(record)

	require.NotNil(t, result)
	assert.NotNil(t, result.Headers)
	assert.Empty(t, result.Headers, "Headers map should be empty but not nil")
}

// ============================================================================
// Тест обратный: FromKafkaMessage - конвертация kafka.Message → kgo.Record
// ============================================================================

func TestFromKafkaMessage(t *testing.T) {
	timestamp := time.Now().Truncate(time.Millisecond)

	msg := &kafka.Message{
		Topic:     "original-topic",
		Partition: 10,
		Offset:    999,
		Timestamp: timestamp,
		Key:       []byte("original-key"),
		Value:     []byte("original-value"),
		Headers: map[string][]byte{
			"x-request-id": []byte("req-123"),
			"x-user-id":    []byte("user-456"),
		},
		Metadata: map[string]interface{}{ // не должен влиять на результат
			"custom": "data",
		},
	}

	result := FromKafkaMessage(msg)

	require.NotNil(t, result)
	assert.Equal(t, "original-topic", result.Topic)
	assert.Equal(t, []byte("original-key"), result.Key)
	assert.Equal(t, []byte("original-value"), result.Value)
	assert.Equal(t, timestamp, result.Timestamp)

	// Проверяем заголовки (срез)
	require.Len(t, result.Headers, 2, "Should have 2 headers")

	// Находим заголовки (порядок может быть разным)
	headersMap := make(map[string][]byte)
	for _, h := range result.Headers {
		headersMap[h.Key] = h.Value
	}
	assert.Equal(t, []byte("req-123"), headersMap["x-request-id"])
	assert.Equal(t, []byte("user-456"), headersMap["x-user-id"])
}

// ============================================================================
// Тест обратный: FromKafkaMessage с nil
// ============================================================================

func TestFromKafkaMessage_NilInput(t *testing.T) {
	result := FromKafkaMessage(nil)
	assert.Nil(t, result, "Should return nil when input is nil")
}

// ============================================================================
// Тест обратный: FromKafkaMessage с пустыми заголовками
// ============================================================================

func TestFromKafkaMessage_EmptyHeaders(t *testing.T) {
	msg := &kafka.Message{
		Topic:   "test",
		Headers: map[string][]byte{},
	}

	result := FromKafkaMessage(msg)

	require.NotNil(t, result)
	assert.Empty(t, result.Headers, "Headers slice should be empty")
	assert.NotNil(t, result.Headers, "Headers slice should not be nil")
}

// ============================================================================
// Тест: ToDLQMessage - конвертация в DLQ структуру
// ============================================================================

func TestToDLQMessage(t *testing.T) {
	timestamp := time.Now().Truncate(time.Millisecond)

	originalMsg := &kafka.Message{
		Topic:     "orders",
		Partition: 5,
		Offset:    100500,
		Timestamp: timestamp,
		Key:       []byte("order-123"),
		Value:     []byte(`{"id":123,"status":"pending"}`),
		Headers: map[string][]byte{
			"content-type": []byte("application/json"),
			"trace-id":     []byte("trace-xyz"),
		},
	}

	processingErr := assert.AnError // стандартная тестовая ошибка
	serviceName := "order-processor"

	result := ToDLQMessage(originalMsg, processingErr, serviceName)

	require.NotNil(t, result, "DLQMessage should not be nil")

	// Проверяем метаданные
	assert.Equal(t, "orders", result.Metadata.OriginalTopic)
	assert.Equal(t, int32(5), result.Metadata.OriginalPartition)
	assert.Equal(t, int64(100500), result.Metadata.OriginalOffset)
	assert.Equal(t, timestamp, result.Metadata.Timestamp)
	assert.Equal(t, processingErr.Error(), result.Metadata.Error)
	assert.Equal(t, "order-processor", result.Metadata.Service)
	assert.Equal(t, 0, result.Metadata.RetryCount, "RetryCount should be 0 by default")

	// Проверяем payload
	assert.Equal(t, "order-123", result.Payload.Key)
	assert.Equal(t, `{"id":123,"status":"pending"}`, result.Payload.Value)

	// Проверяем заголовки в payload
	expectedHeaders := map[string]string{
		"content-type": "application/json",
		"trace-id":     "trace-xyz",
	}
	assert.Equal(t, expectedHeaders, result.Payload.Headers)
}

// ============================================================================
// Тест: ToDLQMessage с nil оригинальным сообщением
// ============================================================================

func TestToDLQMessage_NilOriginalMsg(t *testing.T) {
	result := ToDLQMessage(nil, assert.AnError, "test-service")
	assert.Nil(t, result, "Should return nil when original message is nil")
}

// ============================================================================
// Тест: ToDLQMessage с пустыми заголовками
// ============================================================================

func TestToDLQMessage_EmptyHeaders(t *testing.T) {
	originalMsg := &kafka.Message{
		Topic:   "test",
		Headers: map[string][]byte{},
	}

	result := ToDLQMessage(originalMsg, assert.AnError, "test")

	require.NotNil(t, result)
	assert.Empty(t, result.Payload.Headers, "Headers should be empty")
	assert.NotNil(t, result.Payload.Headers, "Headers map should not be nil")
}

// ============================================================================
// Тест: ToDLQMessage с nil ошибкой (не должно паниковать)
// ============================================================================

func TestToDLQMessage_NilError(t *testing.T) {
	originalMsg := &kafka.Message{
		Topic: "test",
		Value: []byte("data"),
	}

	// ВАЖНО: передаём nil ошибку
	result := ToDLQMessage(originalMsg, nil, "test-service")

	require.NotNil(t, result)
	assert.Equal(t, "", result.Metadata.Error, "Error should be empty string")
	assert.NotPanics(t, func() {
		ToDLQMessage(originalMsg, nil, "test")
	})
}

// ============================================================================
// Дополнительный тест: Целостность конвертации (туда-обратно)
// ============================================================================

func TestConversionRoundtrip(t *testing.T) {
	timestamp := time.Now().Truncate(time.Millisecond)

	originalRecord := &kgo.Record{
		Topic:     "roundtrip-topic",
		Partition: 7,
		Offset:    777,
		Timestamp: timestamp,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers: []kgo.RecordHeader{
			{Key: "h1", Value: []byte("v1")},
			{Key: "h2", Value: []byte("v2")},
		},
	}

	// kgo.Record → kafka.Message
	msg := ToKafkaMessage(originalRecord)

	// kafka.Message → kgo.Record
	convertedBack := FromKafkaMessage(msg)

	// Сравниваем
	assert.Equal(t, originalRecord.Topic, convertedBack.Topic)
	assert.Equal(t, originalRecord.Key, convertedBack.Key)
	assert.Equal(t, originalRecord.Value, convertedBack.Value)
	assert.Equal(t, originalRecord.Timestamp, convertedBack.Timestamp)

	// Заголовки сравниваем как мапу (порядок не важен)
	originalHeaders := make(map[string][]byte)
	for _, h := range originalRecord.Headers {
		originalHeaders[h.Key] = h.Value
	}

	convertedHeaders := make(map[string][]byte)
	for _, h := range convertedBack.Headers {
		convertedHeaders[h.Key] = h.Value
	}

	assert.Equal(t, originalHeaders, convertedHeaders)
}
