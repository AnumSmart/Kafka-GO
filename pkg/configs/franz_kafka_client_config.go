package configs

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaClientConfig - единый конфиг для Kafka клиента (consumer + producer)
type KafkaClientConfig struct {
	// Общие настройки (были в KafkaConfig)
	Brokers      []string      `yaml:"brokers" json:"brokers"`
	Topic        string        `yaml:"topic" json:"topic"`                 // Основной топик для consumer
	GroupID      string        `yaml:"group_id" json:"group_id"`           // Consumer group ID
	BatchSize    int           `yaml:"batch_size" json:"batch_size"`       // Для батчевой обработки
	BatchTimeout time.Duration `yaml:"batch_timeout" json:"batch_timeout"` // Таймаут батча
	MinBytes     int           `yaml:"min_bytes" json:"min_bytes"`         // Fetch min bytes
	MaxBytes     int           `yaml:"max_bytes" json:"max_bytes"`         // Fetch max bytes
	MaxWait      time.Duration `yaml:"max_wait" json:"max_wait"`           // Fetch max wait

	// Consumer специфичные настройки
	Consumer ConsumerConfig `yaml:"consumer" json:"consumer"`

	// Producer специфичные настройки (для DLQ)
	Producer ProducerConfig `yaml:"producer" json:"producer"`
}

// ConsumerConfig - настройки consumer
type ConsumerConfig struct {
	// Enabled - флаг, включён ли consumer
	Enabled bool `yaml:"enabled" json:"enabled"`
	// StartOffset - с какого смещения начинать чтение
	StartOffset string `yaml:"start_offset" json:"start_offset"`
	// CommitInterval - интервал автоматического коммита
	// 0 = ручной коммит (manual commit) - рекомендуется для надёжности
	// >0 = авто-коммит каждые N секунд - проще, но можно потерять сообщения
	CommitInterval time.Duration `yaml:"commit_interval" json:"commit_interval"`
	// SessionTimeout - таймаут сессии (heartbeat)
	// Если consumer не отправит heartbeat за это время, Kafka считает его мёртвым
	// и инициирует ребалансировку (перераспределение партиций)
	SessionTimeout time.Duration `yaml:"session_timeout" json:"session_timeout"`
	// RebalanceTimeout - таймаут ребалансировки
	// Максимальное время, которое consumer может обрабатывать сообщения
	// до того, как партиции будут отозваны
	RebalanceTimeout time.Duration `yaml:"rebalance_timeout" json:"rebalance_timeout"`
	// MaxRecordsPerFetch - максимальное количество записей за один fetch
	// Ограничивает размер батча, чтобы не перегружать память
	MaxRecordsPerFetch int32 `yaml:"max_records_per_fetch" json:"max_records_per_fetch"`
	// DisableAutoCommit - отключить авто-коммит
	// Если true, коммит происходит только явно через CommitUncommittedOffsets
	// Рекомендуется true для ручного контроля
	DisableAutoCommit bool `yaml:"disable_auto_commit" json:"disable_auto_commit"`
}

// ProducerConfig - настройки producer
type ProducerConfig struct {
	Enabled        bool          `yaml:"enabled" json:"enabled"`
	RequiredAcks   int16         `yaml:"required_acks" json:"required_acks"`     // -1, 0, 1
	MaxRetries     int           `yaml:"max_retries" json:"max_retries"`         // RecordRetries
	Idempotent     bool          `yaml:"idempotent" json:"idempotent"`           // true (default) / false
	BatchBytes     int32         `yaml:"batch_bytes" json:"batch_bytes"`         // ProducerBatchMaxBytes
	BatchTimeout   time.Duration `yaml:"batch_timeout" json:"batch_timeout"`     // ProducerLinger
	Compression    string        `yaml:"compression" json:"compression"`         // none, gzip, snappy, lz4, zstd
	Timeout        time.Duration `yaml:"timeout" json:"timeout"`                 // RecordDeliveryTimeout
	RequestTimeout time.Duration `yaml:"request_timeout" json:"request_timeout"` // ProduceRequestTimeout

	// MaxRecordSizeBytes – используется только для валидации на уровне приложения,
	// т.к. franz-go не имеет отдельной опции для максимального размера одной записи.
	// Запись, превышающая ProducerBatchMaxBytes, будет отклонена с kerr.MessageTooLarge.
	MaxRecordSizeBytes int       `yaml:"max_record_size_bytes" json:"max_record_size_bytes"`
	DLQ                DLQConfig `yaml:"dlq" json:"dlq"`
}

// DLQConfig - настройки Dead Letter Queue
type DLQConfig struct {
	Enabled             bool          `yaml:"enabled" json:"enabled"`
	TopicSuffix         string        `yaml:"topic_suffix" json:"topic_suffix"`
	IncludeErrorHeaders bool          `yaml:"include_error_headers" json:"include_error_headers"`
	SendTimeout         time.Duration `yaml:"send_timeout" json:"send_timeout"`
	MaxMessageSize      int           `yaml:"max_message_size" json:"max_message_size"`
}

// DefaultKafkaClientConfig - дефолтная конфигурация для единого клиента
func DefaultKafkaClientConfig() *KafkaClientConfig {
	return &KafkaClientConfig{
		// Общие настройки
		Brokers:      []string{"localhost:9092"},
		Topic:        "default-topic",
		GroupID:      "default-group",
		BatchSize:    100,
		BatchTimeout: 5 * time.Second,
		MinBytes:     1,
		MaxBytes:     10e6, // 10MB
		MaxWait:      1 * time.Second,

		// Consumer настройки
		Consumer: ConsumerConfig{
			Enabled:            true,
			StartOffset:        "latest",
			CommitInterval:     0, // 0 = ручной коммит
			SessionTimeout:     30 * time.Second,
			RebalanceTimeout:   60 * time.Second,
			MaxRecordsPerFetch: 1000,
			DisableAutoCommit:  true,
		},

		// Producer настройки
		Producer: ProducerConfig{
			Enabled:            true,
			RequiredAcks:       -1,
			MaxRetries:         3,
			Idempotent:         true,
			BatchBytes:         16384,
			BatchTimeout:       1 * time.Second,
			Compression:        "snappy",
			Timeout:            10 * time.Second,
			RequestTimeout:     5 * time.Second,
			MaxRecordSizeBytes: 1048576,
			DLQ: DLQConfig{
				Enabled:             false,
				TopicSuffix:         ".dlq",
				IncludeErrorHeaders: true,
				SendTimeout:         5 * time.Second,
				MaxMessageSize:      1048576,
			},
		},
	}
}

// ToKgoOptions - конвертирует единый конфиг в опции kgo.Client
func (c *KafkaClientConfig) ToKgoOptions() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		// Базовые настройки
		kgo.SeedBrokers(c.Brokers...),
	}

	// Добавляем consumer настройки, если consumer включен
	if c.Consumer.Enabled {
		consumerOpts, err := c.getConsumerOptions()
		if err != nil {
			return nil, err
		}
		opts = append(opts, consumerOpts...)
	}

	// Добавляем producer настройки, если producer включен (для DLQ)
	if c.Producer.Enabled {
		producerOpts, err := c.getProducerOptions()
		if err != nil {
			return nil, err
		}
		opts = append(opts, producerOpts...)
	}

	return opts, nil
}

// getConsumerOptions - получает опции для consumer
func (c *KafkaClientConfig) getConsumerOptions() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.FetchMinBytes(int32(c.MinBytes)),
		kgo.FetchMaxBytes(int32(c.MaxBytes)),
		kgo.FetchMaxWait(c.MaxWait),
		kgo.SessionTimeout(c.Consumer.SessionTimeout),
		kgo.RebalanceTimeout(c.Consumer.RebalanceTimeout),
	}

	// Настройка offset
	startOffset, err := c.getStartOffset()
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ConsumeResetOffset(startOffset))

	// Настройка авто-коммита
	if c.Consumer.DisableAutoCommit || c.Consumer.CommitInterval == 0 {
		opts = append(opts, kgo.DisableAutoCommit())
	} else {
		opts = append(opts, kgo.AutoCommitInterval(c.Consumer.CommitInterval))
	}

	return opts, nil
}

// getProducerOptions - получает опции для producer
func (c *KafkaClientConfig) getProducerOptions() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.RequiredAcks(c.getRequiredAcks()),
		kgo.RecordRetries(c.Producer.MaxRetries),
		kgo.ProducerBatchMaxBytes(c.Producer.BatchBytes),
		kgo.ProducerLinger(c.Producer.BatchTimeout),
		kgo.RecordDeliveryTimeout(c.Producer.Timeout),
		kgo.ProduceRequestTimeout(c.Producer.RequestTimeout),
	}

	// Idempotency
	if !c.Producer.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	// Compression
	compOpt, err := c.getCompressionOpt()
	if err != nil {
		return nil, err
	}
	if compOpt != nil {
		opts = append(opts, compOpt)
	}

	return opts, nil
}

// getStartOffset - преобразует строковый offset в формат franz-go
func (c *KafkaClientConfig) getStartOffset() (kgo.Offset, error) {
	switch c.Consumer.StartOffset {
	case "earliest", "at_start":
		return kgo.NewOffset().AtStart(), nil
	case "latest", "at_end":
		return kgo.NewOffset().AtEnd(), nil
	default:
		return kgo.NewOffset().AtEnd(), fmt.Errorf("invalid start_offset: %s", c.Consumer.StartOffset)
	}
}

// getRequiredAcks - преобразует int16 в kgo.Acks
func (c *KafkaClientConfig) getRequiredAcks() kgo.Acks {
	switch c.Producer.RequiredAcks {
	case -1:
		return kgo.AllISRAcks()
	case 0:
		return kgo.NoAck()
	case 1:
		return kgo.LeaderAck()
	default:
		return kgo.LeaderAck()
	}
}

// getCompressionOpt - возвращает опцию компрессии
func (c *KafkaClientConfig) getCompressionOpt() (kgo.ProducerOpt, error) {
	switch c.Producer.Compression {
	case "", "none":
		return nil, nil
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), nil
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), nil
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), nil
	case "zstd":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), nil
	default:
		return nil, fmt.Errorf("invalid compression: %s", c.Producer.Compression)
	}
}

func (c *KafkaClientConfig) IsDLQEnabled() bool {
	if !c.Producer.DLQ.Enabled {
		return false
	}
	return true
}

// GetDLQTopic - формирует имя топика для DLQ
func (c *KafkaClientConfig) GetDLQTopic() string {
	if c.Producer.DLQ.TopicSuffix == "" {
		return c.Topic + ".dlq"
	}
	return c.Topic + c.Producer.DLQ.TopicSuffix
}

// Validate - валидация конфигурации
func (c *KafkaClientConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers list cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.GroupID == "" {
		return fmt.Errorf("group_id cannot be empty")
	}

	// Consumer validation
	if c.Consumer.Enabled {
		if c.Consumer.SessionTimeout <= 0 {
			return fmt.Errorf("session_timeout must be > 0")
		}
		if c.Consumer.MaxRecordsPerFetch <= 0 {
			return fmt.Errorf("max_records_per_fetch must be >= 1")
		}
	}

	// Producer validation
	if c.Producer.Enabled {
		if c.Producer.RequiredAcks < -1 || c.Producer.RequiredAcks > 1 {
			return fmt.Errorf("required_acks must be -1, 0, or 1")
		}
		if c.Producer.MaxRetries < 0 {
			return fmt.Errorf("max_retries must be >= 0")
		}
	}

	return nil
}

// LoadKafkaClientConfig - загружает конфиг из YAML файла
func LoadKafkaClientConfig(configPath string) (*KafkaClientConfig, error) {
	return LoadYAMLConfig[KafkaClientConfig](configPath, DefaultKafkaClientConfig)
}
