package kafkagosegmproducer

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Создаём kafka writer из kafka конфига
func NewProducerFromConfig(conf kafka.WriterConfig) (*kafka.Writer, error) {

	// проверяем наличие балансировщика (страхуемся)
	if conf.Balancer == nil {
		return nil, fmt.Errorf("kafka writer config: Balancer cannot be nil (would cause panic)")
	}

	// создаём writer
	writer := kafka.NewWriter(conf)

	return writer, nil
}
