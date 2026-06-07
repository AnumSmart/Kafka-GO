package kafka

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
	writer.AllowAutoTopicCreation = false // принудительно отключаем автосоздание топика

	// Добавьте лог для проверки настроек
	fmt.Printf("Writer created: brokers=%v, topic=%s, allowAutoCreate=%v\n",
		conf.Brokers, conf.Topic, writer.AllowAutoTopicCreation)

	return writer, nil
}
