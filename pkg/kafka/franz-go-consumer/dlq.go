package franzgoconsumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DLQMessage - структура сообщения для DLQ
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
}

type DLQPayload struct {
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

// dlqManager - реализация DLQSender
type dlqManager struct {
	producer *kgo.Client
	topic    string
	enabled  bool
}

// NewDLQManager - конструктор для DI (принимает готовый producer)
func NewDLQManager(producer *kgo.Client, topic string, enabled bool) DLQSender {
	log.Printf("🔧 Initializing DLQ manager: enabled=%v, topic=%s, producer=%v",
		enabled, topic, producer != nil)

	return &dlqManager{
		producer: producer,
		topic:    topic,
		enabled:  enabled,
	}
}

// Send - отправляет сообщение в DLQ
func (dm *dlqManager) Send(ctx context.Context, originalRecord *kgo.Record, handlingErr error) error {
	// Сценарий 1: DLQ полностью отключен
	if !dm.enabled {
		log.Printf("⚠️ DLQ is disabled, skipping message: topic=%s, partition=%d, offset=%d, error=%v",
			originalRecord.Topic, originalRecord.Partition, originalRecord.Offset, handlingErr)
		return nil
	}

	// Сценарий 2: DLQ включен, но producer не инициализирован
	if dm.producer == nil {
		log.Printf("❌ DLQ is enabled but producer is nil, cannot send message: topic=%s, offset=%d, error=%v",
			originalRecord.Topic, originalRecord.Offset, handlingErr)
		return nil
	}

	// Сценарий 3: Отсутствует топик
	if dm.topic == "" {
		log.Printf("❌ DLQ topic is empty, cannot send message: topic=%s, offset=%d, error=%v",
			originalRecord.Topic, originalRecord.Offset, handlingErr)
		return nil
	}

	// Сценарий 4: Нормальная отправка
	log.Printf("📨 Preparing to send message to DLQ: original_topic=%s, original_offset=%d, dlq_topic=%s, error=%v",
		originalRecord.Topic, originalRecord.Offset, dm.topic, handlingErr)

	dlqMsg := dm.buildDLQMessage(originalRecord, handlingErr)

	value, err := json.Marshal(dlqMsg)
	if err != nil {
		log.Printf("❌ Failed to marshal DLQ message: %v", err)
		return err
	}

	dlqRecord := &kgo.Record{
		Topic: dm.topic,
		Key:   originalRecord.Key,
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "original_topic", Value: []byte(originalRecord.Topic)},
			{Key: "original_error", Value: []byte(handlingErr.Error())},
		},
	}

	// Отправка с таймаутом
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := dm.producer.ProduceSync(sendCtx, dlqRecord).FirstErr(); err != nil {
		log.Printf("❌ Failed to send to DLQ: original_topic=%s, original_offset=%d, dlq_topic=%s, error=%v",
			originalRecord.Topic, originalRecord.Offset, dm.topic, err)
		return err
	}

	log.Printf("✅ Successfully sent to DLQ: original_topic=%s, original_offset=%d, dlq_topic=%s",
		originalRecord.Topic, originalRecord.Offset, dm.topic)

	return nil
}

// Close - закрывает DLQ producer
func (dm *dlqManager) Close() error {
	if dm.producer == nil {
		log.Printf("⚠️ DLQ Close called but producer is nil, nothing to close")
		return nil
	}

	if !dm.enabled {
		log.Printf("⚠️ DLQ Close called but DLQ is disabled, closing producer anyway")
	}

	log.Println("🛑 Closing DLQ producer...")
	dm.producer.Close()
	log.Println("✅ DLQ producer closed")
	return nil
}

// IsEnabled - возвращает статус DLQ с пояснением в логах при первом вызове
func (dm *dlqManager) IsEnabled() bool {
	enabled := dm.enabled && dm.producer != nil && dm.topic != ""

	if !enabled {
		log.Printf("🔍 DLQ status check: enabled=%v, producer_exists=%v, topic_set=%v",
			dm.enabled, dm.producer != nil, dm.topic != "")
	}

	return enabled
}

// buildDLQMessage - формирует сообщение
func (dm *dlqManager) buildDLQMessage(record *kgo.Record, err error) DLQMessage {
	headers := make(map[string]string)
	for _, h := range record.Headers {
		headers[h.Key] = string(h.Value)
	}

	return DLQMessage{
		Metadata: DLQMetadata{
			OriginalTopic:     record.Topic,
			OriginalPartition: record.Partition,
			OriginalOffset:    record.Offset,
			Timestamp:         time.Now(),
			Error:             err.Error(),
			Service:           "kafka-consumer",
		},
		Payload: DLQPayload{
			Key:     string(record.Key),
			Value:   string(record.Value),
			Headers: headers,
		},
	}
}
