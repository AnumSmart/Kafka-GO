package configs

import (
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerConfigKafkaGo - минимальная конфигурация консьюмера для kafka-go
type ConsumerConfigKafkaGo struct {
	// Встраиваем базовый Kafka конфиг
	KafkaConfig `yaml:",inline"`

	// Специфичные настройки консьюмера
	ConsumerSpecific ConsumerSpecificConfig `yaml:"consumer" json:"consumer"`
}

// ConsumerSpecificConfig - минимальные настройки консьюмера
type ConsumerSpecificConfig struct {
	Enabled        bool          `yaml:"enabled" json:"enabled"`
	StartOffset    string        `yaml:"start_offset" json:"start_offset"`       // "earliest", "latest"
	CommitInterval time.Duration `yaml:"commit_interval" json:"commit_interval"` // интервал авто-коммита (0 = синхронный)
	QueueCapacity  int           `yaml:"queue_capacity" json:"queue_capacity"`   // размер очереди сообщений
}

// DefaultConsumerConfig - дефолтный конфиг для консьюмера
func DefaultConsumerConfig() *ConsumerConfigKafkaGo {
	return &ConsumerConfigKafkaGo{
		KafkaConfig: *DefaultKafkaConfig(),
		ConsumerSpecific: ConsumerSpecificConfig{
			Enabled:        true,
			StartOffset:    "earliest",
			CommitInterval: 1 * time.Second,
			QueueCapacity:  100,
		},
	}
}

// ToKafkaReaderConfig - конвертирует в конфиг kafka-go Reader
func (c *ConsumerConfigKafkaGo) ToKafkaReaderConfig() kafka.ReaderConfig {
	startOffset := getStartOffset(c.ConsumerSpecific.StartOffset)

	return kafka.ReaderConfig{
		// Базовые настройки
		Brokers:  c.Brokers,
		Topic:    c.Topic,
		GroupID:  c.GroupID,
		MinBytes: c.MinBytes,
		MaxBytes: c.MaxBytes,
		MaxWait:  c.MaxWait,

		// Настройки консьюмера
		StartOffset:    startOffset,
		CommitInterval: c.ConsumerSpecific.CommitInterval,
		QueueCapacity:  c.ConsumerSpecific.QueueCapacity,
	}
}

// Validate - минимальная валидация
func (c *ConsumerConfigKafkaGo) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers list cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.GroupID == "" {
		return fmt.Errorf("group_id cannot be empty")
	}
	if c.ConsumerSpecific.StartOffset != "earliest" && c.ConsumerSpecific.StartOffset != "latest" {
		return fmt.Errorf("start_offset must be 'earliest' or 'latest'")
	}
	if c.ConsumerSpecific.QueueCapacity < 1 {
		return fmt.Errorf("queue_capacity must be >= 1")
	}
	return nil
}

// LoadConsumerConfig - загружает конфиг консьюмера из YAML файла
func LoadConsumerConfig(configPath string) (*ConsumerConfigKafkaGo, error) {
	return LoadYAMLConfig[ConsumerConfigKafkaGo](configPath, DefaultConsumerConfig)
}

// ========== Вспомогательные функции ==========

func getStartOffset(startOffset string) int64 {
	switch startOffset {
	case "latest":
		return kafka.LastOffset
	default:
		return kafka.FirstOffset
	}
}
