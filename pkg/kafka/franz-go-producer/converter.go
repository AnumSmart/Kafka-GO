package franzgoproducer

import (
	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
		Metadata:  make(map[string]interface{}),
	}
}

// BatchFromKafkaMessages - конвертирует слайс общих сообщений в слайс записей
func BatchFromKafkaMessages(messages []*kafka.Message) []*kgo.Record {
	if len(messages) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(messages))
	for _, msg := range messages {
		records = append(records, FromKafkaMessage(msg))
	}
	return records
}

// BatchToKafkaMessages - конвертирует слайс записей в слайс общих сообщений
func BatchToKafkaMessages(records []*kgo.Record) []*kafka.Message {
	if len(records) == 0 {
		return nil
	}

	messages := make([]*kafka.Message, 0, len(records))
	for _, record := range records {
		messages = append(messages, ToKafkaMessage(record))
	}
	return messages
}
