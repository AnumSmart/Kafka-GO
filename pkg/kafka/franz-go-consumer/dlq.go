package franzgoconsumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// dlqManager - реализация kafka.DLQSender
type dlqManager struct {
	producer    *kgo.Client
	topic       string
	enabled     bool
	serviceName string
}

// NewDLQManager - конструктор для DI (принимает готовый producer)
func NewDLQManager(producer *kgo.Client, topic string, enabled bool) kafka.DLQSender {
	log.Printf("🔧 Initializing DLQ manager: enabled=%v, topic=%s, producer=%v",
		enabled, topic, producer != nil)

	return &dlqManager{
		producer:    producer,
		topic:       topic,
		enabled:     enabled,
		serviceName: "kafka-consumer", // можно сделать настраиваемым через опции
	}
}

// Send - отправляет сообщение в DLQ (реализация kafka.DLQSender)
func (dm *dlqManager) Send(ctx context.Context, originalMsg *kafka.Message, handlingErr error) error {
	// Сценарий 1: DLQ полностью отключен
	if !dm.enabled {
		log.Printf("⚠️ DLQ is disabled, skipping message: topic=%s, partition=%d, offset=%d, error=%v",
			originalMsg.Topic, originalMsg.Partition, originalMsg.Offset, handlingErr)
		return nil
	}

	// Сценарий 2: DLQ включен, но producer не инициализирован
	if dm.producer == nil {
		log.Printf("❌ DLQ is enabled but producer is nil, cannot send message: topic=%s, offset=%d, error=%v",
			originalMsg.Topic, originalMsg.Offset, handlingErr)
		return kafka.ErrDLQSendFailed
	}

	// Сценарий 3: Отсутствует топик
	if dm.topic == "" {
		log.Printf("❌ DLQ topic is empty, cannot send message: topic=%s, offset=%d, error=%v",
			originalMsg.Topic, originalMsg.Offset, handlingErr)
		return kafka.ErrDLQSendFailed
	}

	// Сценарий 4: Нормальная отправка
	log.Printf("📨 Preparing to send message to DLQ: original_topic=%s, original_offset=%d, dlq_topic=%s, error=%v",
		originalMsg.Topic, originalMsg.Offset, dm.topic, handlingErr)

	// Используем конвертер для создания DLQ сообщения
	dlqMsg := ToDLQMessage(originalMsg, handlingErr, dm.serviceName)

	value, err := json.Marshal(dlqMsg)
	if err != nil {
		log.Printf("❌ Failed to marshal DLQ message: %v", err)
		return err
	}

	// Создаем запись для Kafka
	dlqRecord := &kgo.Record{
		Topic: dm.topic,
		Key:   originalMsg.Key,
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "original_topic", Value: []byte(originalMsg.Topic)},
			{Key: "original_error", Value: []byte(handlingErr.Error())},
			{Key: "original_offset", Value: []byte(itoa(originalMsg.Offset))},
			{Key: "original_partition", Value: []byte(itoa(int64(originalMsg.Partition)))},
		},
	}

	// Отправка с таймаутом
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := dm.producer.ProduceSync(sendCtx, dlqRecord).FirstErr(); err != nil {
		log.Printf("❌ Failed to send to DLQ: original_topic=%s, original_offset=%d, dlq_topic=%s, error=%v",
			originalMsg.Topic, originalMsg.Offset, dm.topic, err)
		return err
	}

	log.Printf("✅ Successfully sent to DLQ: original_topic=%s, original_offset=%d, dlq_topic=%s",
		originalMsg.Topic, originalMsg.Offset, dm.topic)

	return nil
}

// Close - закрывает DLQ producer (реализация kafka.DLQSender)
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

// IsEnabled - возвращает статус DLQ (реализация kafka.DLQSender)
func (dm *dlqManager) IsEnabled() bool {
	enabled := dm.enabled && dm.producer != nil && dm.topic != ""

	if !enabled && dm.enabled {
		// Логируем только если DLQ включен, но не готов к работе
		log.Printf("🔍 DLQ status check: enabled=%v, producer_exists=%v, topic_set=%v",
			dm.enabled, dm.producer != nil, dm.topic != "")
	}

	return enabled
}

// itoa - простой конвертер int64 в string (избегаем зависимости от strconv)
func itoa(i int64) string {
	if i == 0 {
		return "0"
	}

	negative := false
	if i < 0 {
		negative = true
		i = -i
	}

	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}

	if negative {
		return "-" + string(digits)
	}
	return string(digits)
}
