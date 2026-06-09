package franzgoproducer

// ProducerOptions - внутренние настройки продьюсера
// Эти опции используются для базовой конфигурации, не связанной с Kafka клиентом
type ProducerOptions struct {
	// Topic - топик по умолчанию (может быть переопределен в сообщении)
	Topic string

	// EnableDebugLog - включить отладочное логирование
	EnableDebugLog bool
}

// DefaultOptions - настройки по умолчанию
func DefaultOptions() *ProducerOptions {
	return &ProducerOptions{
		Topic:          "",
		EnableDebugLog: false,
	}
}

// Validate - валидация настроек
func (o *ProducerOptions) Validate() error {
	// Минимальная валидация, т.к. основные настройки в configs
	return nil
}
