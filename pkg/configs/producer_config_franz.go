package configs

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FranzProducerConfig - конфигурация продьюсера для franz-go
type FranzProducerConfig struct {
	KafkaConfig      `yaml:",inline"`
	ProducerSpecific FranzProducerSpecificConfig `yaml:"producer" json:"producer"`
}

// FranzProducerSpecificConfig - настройки продьюсера для franz-go
type FranzProducerSpecificConfig struct {
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
	MaxRecordSizeBytes int `yaml:"max_record_size_bytes" json:"max_record_size_bytes"`

	DLQ DLQProducerConfig `yaml:"dlq" json:"dlq,omitempty"`
}

// DLQProducerConfig – без изменений, не влияет на kgo опции
type DLQProducerConfig struct {
	TopicSuffix         string        `yaml:"topic_suffix" json:"topic_suffix"`
	IncludeErrorHeaders bool          `yaml:"include_error_headers" json:"include_error_headers"`
	SendTimeout         time.Duration `yaml:"send_timeout" json:"send_timeout"`
	MaxMessageSize      int           `yaml:"max_message_size" json:"max_message_size"`
}

func DefaultFranzProducerConfig() *FranzProducerConfig {
	return &FranzProducerConfig{
		KafkaConfig: *DefaultKafkaConfig(), // предполагается, что там поля Brokers, Topic и т.д.
		ProducerSpecific: FranzProducerSpecificConfig{
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
			DLQ: DLQProducerConfig{
				TopicSuffix:         ".dlq",
				IncludeErrorHeaders: true,
				SendTimeout:         5 * time.Second,
				MaxMessageSize:      1048576,
			},
		},
	}
}

// getRequiredAcks преобразует int16 в kgo.Acks
func (c *FranzProducerConfig) getRequiredAcks() kgo.Acks {
	switch c.ProducerSpecific.RequiredAcks {
	case -1:
		return kgo.AllISRAcks()
	case 0:
		return kgo.NoAck()
	case 1:
		return kgo.LeaderAck()
	default:
		// fallback на LeaderAck (по аналогии с библиотекой)
		return kgo.LeaderAck()
	}
}

// getCompressionOpt возвращает опцию ProducerBatchCompression для заданного кодека
func (c *FranzProducerConfig) getCompressionOpt() (kgo.ProducerOpt, error) {
	codecStr := c.ProducerSpecific.Compression
	switch codecStr {
	case "", "none":
		return nil, nil // без компрессии
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), nil
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), nil
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), nil
	case "zstd":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), nil
	default:
		return nil, fmt.Errorf("invalid compression codec: %s (must be: none, gzip, snappy, lz4, zstd)", codecStr)
	}
}

// ToKgoOptions преобразует конфиг в список опций kgo.Client
func (c *FranzProducerConfig) ToKgoOptions() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.RequiredAcks(c.getRequiredAcks()),
		kgo.RecordRetries(c.ProducerSpecific.MaxRetries),
		kgo.ProducerBatchMaxBytes(c.ProducerSpecific.BatchBytes),
		kgo.ProducerLinger(c.ProducerSpecific.BatchTimeout),
		kgo.RecordDeliveryTimeout(c.ProducerSpecific.Timeout),
		kgo.ProduceRequestTimeout(c.ProducerSpecific.RequestTimeout),
	}

	// Idempotency – включена по умолчанию. Отключаем, если явно указано false.
	if !c.ProducerSpecific.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	// Компрессия (если нужна)
	compOpt, err := c.getCompressionOpt()
	if err != nil {
		return nil, err
	}
	if compOpt != nil {
		opts = append(opts, compOpt)
	}

	// Дополнительно: можно задать максимальное количество буферизуемых записей/байт,
	// если требуется. Оставляем на усмотрение – стандартные значения подходят для многих сценариев.
	// Пример:
	// opts = append(opts, kgo.MaxBufferedRecords(10000))
	// opts = append(opts, kgo.MaxBufferedBytes(100*1024*1024))

	return opts, nil
}

// Validate – обновлённая валидация (убрали RetryBackoff, добавили проверки для новых опций)
func (c *FranzProducerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers list cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.ProducerSpecific.RequiredAcks < -1 || c.ProducerSpecific.RequiredAcks > 1 {
		return fmt.Errorf("required_acks must be -1, 0, or 1, got: %d", c.ProducerSpecific.RequiredAcks)
	}
	if c.ProducerSpecific.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0")
	}
	if c.ProducerSpecific.BatchBytes <= 0 {
		return fmt.Errorf("batch_bytes must be > 0")
	}
	if c.ProducerSpecific.BatchTimeout <= 0 {
		return fmt.Errorf("batch_timeout must be > 0")
	}
	if c.ProducerSpecific.Timeout <= 0 {
		return fmt.Errorf("timeout must be > 0")
	}
	if c.ProducerSpecific.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be > 0")
	}
	if c.ProducerSpecific.MaxRecordSizeBytes <= 0 {
		return fmt.Errorf("max_record_size_bytes must be > 0")
	}
	// Compression валидируется при преобразовании в опцию, но можно добавить быструю проверку:
	validComp := map[string]bool{"none": true, "gzip": true, "snappy": true, "lz4": true, "zstd": true, "": true}
	if !validComp[c.ProducerSpecific.Compression] {
		return fmt.Errorf("invalid compression codec: %s", c.ProducerSpecific.Compression)
	}
	return nil
}

// GetDLQTopic – без изменений
func (c *FranzProducerConfig) GetDLQTopic(originalTopic string) string {
	if c.Topic != "" && c.ProducerSpecific.DLQ.TopicSuffix == "" {
		return c.Topic + c.ProducerSpecific.DLQ.TopicSuffix
	}
	return originalTopic + c.ProducerSpecific.DLQ.TopicSuffix
}

// LoadFranzProducerConfig – без изменений
func LoadFranzProducerConfig(configPath string) (*FranzProducerConfig, error) {
	return LoadYAMLConfig[FranzProducerConfig](configPath, DefaultFranzProducerConfig)
}
