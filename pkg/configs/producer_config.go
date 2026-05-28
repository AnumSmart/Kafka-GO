package configs

import (
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

// ProducerConfig - минимальная конфигурация продюсера для kafka-go
type ProducerConfig struct {
	KafkaConfig      `yaml:",inline"`
	ProducerSpecific ProducerSpecificConfig `yaml:"producer" json:"producer"`
}

type ProducerSpecificConfig struct {
	Enabled      bool   `yaml:"enabled" json:"enabled"`
	Compression  string `yaml:"compression" json:"compression"`     // "gzip", "snappy", "lz4", "zstd", "none"
	RequiredAcks string `yaml:"required_acks" json:"required_acks"` // "none", "one", "all"
	MaxAttempts  int    `yaml:"max_attempts" json:"max_attempts"`   // количество попыток
	Async        bool   `yaml:"async" json:"async"`                 // асинхронная отправка
}

func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		KafkaConfig: *DefaultKafkaConfig(),
		ProducerSpecific: ProducerSpecificConfig{
			Enabled:      true,
			Compression:  "snappy",
			RequiredAcks: "all",
			MaxAttempts:  3,
			Async:        false,
		},
	}
}

func (c *ProducerConfig) ToKafkaWriterConfig() kafka.WriterConfig {
	return kafka.WriterConfig{
		Brokers:          c.Brokers,
		Topic:            c.Topic,
		Balancer:         &kafka.LeastBytes{},
		CompressionCodec: getCompressionCodec(c.ProducerSpecific.Compression),
		RequiredAcks:     getRequiredAcks(c.ProducerSpecific.RequiredAcks),
		MaxAttempts:      c.ProducerSpecific.MaxAttempts,
		BatchSize:        c.BatchSize,
		BatchTimeout:     c.BatchTimeout,
		Async:            c.ProducerSpecific.Async,
	}
}

func (c *ProducerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers list cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	validCompression := map[string]bool{"gzip": true, "snappy": true, "lz4": true, "zstd": true, "none": true}
	if !validCompression[c.ProducerSpecific.Compression] {
		return fmt.Errorf("invalid compression: %s", c.ProducerSpecific.Compression)
	}
	validAcks := map[string]bool{"none": true, "one": true, "all": true}
	if !validAcks[c.ProducerSpecific.RequiredAcks] {
		return fmt.Errorf("invalid required_acks: %s", c.ProducerSpecific.RequiredAcks)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("batch_size must be >= 1")
	}
	return nil
}

func LoadProducerConfig(configPath string) (*ProducerConfig, error) {
	return LoadYAMLConfig(configPath, DefaultProducerConfig)
}

// Вспомогательные функции
func getCompressionCodec(compression string) kafka.CompressionCodec {
	switch compression {
	case "gzip":
		return &compress.GzipCodec
	case "snappy":
		return &compress.SnappyCodec
	case "lz4":
		return &compress.Lz4Codec
	case "zstd":
		return &compress.ZstdCodec
	default:
		return nil
	}
}

func getRequiredAcks(acks string) int {
	switch acks {
	case "none":
		return 0
	case "one":
		return 1
	default:
		return -1
	}
}
