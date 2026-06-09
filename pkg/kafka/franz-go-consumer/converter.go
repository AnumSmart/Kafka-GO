package franzgoconsumer

import (
	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ToKafkaMessage - конвертирует kgo.Record в общее kafka.Message
func ToKafkaMessage(record *kgo.Record) *kafka.Message {
	if record == nil {
		return nil
	}

	headers := make(map[string][]byte)
	for _, h := range record.Headers {
		headers[h.Key] = h.Value
	}

	return &kafka.Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
		Key:       record.Key,
		Value:     record.Value,
		Headers:   headers,
		Metadata:  make(map[string]interface{}), // инициализируем пустым map
	}
}

// FromKafkaMessage - конвертирует общее kafka.Message в kgo.Record
func FromKafkaMessage(msg *kafka.Message) *kgo.Record {
	if msg == nil {
		return nil
	}

	headers := make([]kgo.RecordHeader, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kgo.RecordHeader{
			Key:   k,
			Value: v,
		})
	}

	return &kgo.Record{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Timestamp: msg.Timestamp,
	}
}

// ToDLQMessage - конвертирует оригинальное сообщение и ошибку в DLQMessage
func ToDLQMessage(originalMsg *kafka.Message, err error, serviceName string) *kafka.DLQMessage {
	if originalMsg == nil {
		return nil
	}

	// Конвертируем заголовки из map[[]byte] в map[string]string
	headers := make(map[string]string)
	for k, v := range originalMsg.Headers {
		headers[k] = string(v)
	}

	return &kafka.DLQMessage{
		Metadata: kafka.DLQMetadata{
			OriginalTopic:     originalMsg.Topic,
			OriginalPartition: originalMsg.Partition,
			OriginalOffset:    originalMsg.Offset,
			Timestamp:         originalMsg.Timestamp,
			Error:             err.Error(),
			Service:           serviceName,
		},
		Payload: kafka.DLQPayload{
			Key:     string(originalMsg.Key),
			Value:   string(originalMsg.Value),
			Headers: headers,
		},
	}
}

// BatchToKafkaMessages - конвертирует итератор записей в слайс общих сообщений
func BatchToKafkaMessages(iter *kgo.RecordIter) []*kafka.Message {
	if iter == nil {
		return nil
	}

	var messages []*kafka.Message
	for !iter.Done() {
		record := iter.Next()
		messages = append(messages, ToKafkaMessage(record))
	}
	return messages
}
