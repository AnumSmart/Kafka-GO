package configs

import "time"

// KafkaConfig - базовые настройки подключения к Kafka
type KafkaConfig struct {
	Brokers      []string      `yaml:"brokers" json:"brokers"`             // Список адресов брокеров Kafka (host:port)
	Topic        string        `yaml:"topic" json:"topic"`                 // Имя топика для чтения/записи
	GroupID      string        `yaml:"group_id" json:"group_id"`           // Идентификатор группы потребителей
	BatchSize    int           `yaml:"batch_size" json:"batch_size"`       // Кол-во сообщений в одном пакете для батчевой обработки
	BatchTimeout time.Duration `yaml:"batch_timeout" json:"batch_timeout"` // Макс. время ожидания заполнения батча
	MinBytes     int           `yaml:"min_bytes" json:"min_bytes"`         // Минимум байт для возврата читателем (уменьшает опрос)
	MaxBytes     int           `yaml:"max_bytes" json:"max_bytes"`         // Максимум байт на одно сообщение от брокера
	MaxWait      time.Duration `yaml:"max_wait" json:"max_wait"`           // Макс. время ожидания ответа от брокера
}

// DefaultKafkaConfig - дефолтные значения для Kafka
func DefaultKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "default-topic",
		GroupID:      "default-group",
		BatchSize:    100,
		BatchTimeout: 5 * time.Second,
		MinBytes:     1,
		MaxBytes:     10e6, // 10MB
		MaxWait:      1 * time.Second,
	}
}
