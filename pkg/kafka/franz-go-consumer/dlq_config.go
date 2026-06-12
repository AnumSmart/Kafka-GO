package franzgoconsumer

import (
	"pkg/configs"
	"time"
)

// DLQManagerConfig - расширенная конфигурация для DLQ менеджера
// Используется вместе с configs.KafkaClientConfig.DLQ (базовые настройки)
type DLQManagerConfig struct {
	// Базовые настройки (могут быть переопределены из configs.KafkaClientConfig.DLQ)
	Enabled             bool          `yaml:"enabled" json:"enabled"`
	TopicSuffix         string        `yaml:"topic_suffix" json:"topic_suffix"`
	IncludeErrorHeaders bool          `yaml:"include_error_headers" json:"include_error_headers"`
	SendTimeout         time.Duration `yaml:"send_timeout" json:"send_timeout"`
	MaxMessageSize      int           `yaml:"max_message_size" json:"max_message_size"`

	// Асинхронные настройки (специфичные для DLQ менеджера)
	AsyncMode bool `yaml:"async_mode" json:"async_mode"` // true = асинхронный, false = синхронный
	QueueSize int  `yaml:"queue_size" json:"queue_size"` // размер буфера канала
	Workers   int  `yaml:"workers" json:"workers"`       // количество воркеров
	BatchSize int  `yaml:"batch_size" json:"batch_size"` // размер батча для отправки

	// Таймауты и ретраи
	FlushTimeout time.Duration `yaml:"flush_timeout" json:"flush_timeout"` // таймаут при остановке
	MaxRetries   int           `yaml:"max_retries" json:"max_retries"`     // количество ретраев
	RetryBackoff time.Duration `yaml:"retry_backoff" json:"retry_backoff"` // задержка между ретраями

	// Fallback настройки (опционально)
	FallbackEnabled bool   `yaml:"fallback_enabled" json:"fallback_enabled"`   // включить fallback при полном буфере
	FallbackLogPath string `yaml:"fallback_log_path" json:"fallback_log_path"` // путь для fallback лога

	// Сервис
	ServiceName string `yaml:"service_name" json:"service_name"` // имя сервиса для метаданных
}

// DefaultDLQManagerConfig - конфиг по умолчанию
func DefaultDLQManagerConfig() *DLQManagerConfig {
	return &DLQManagerConfig{
		// Базовые настройки
		Enabled:             false,
		TopicSuffix:         ".dlq",
		IncludeErrorHeaders: true,
		SendTimeout:         5 * time.Second,
		MaxMessageSize:      1048576,

		// Асинхронные настройки
		AsyncMode: true,
		QueueSize: 1000,
		Workers:   3,
		BatchSize: 10,

		// Таймауты и ретраи
		FlushTimeout: 30 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,

		// Fallback
		FallbackEnabled: false,
		FallbackLogPath: "",

		// Сервис
		ServiceName: "kafka-consumer",
	}
}

// NewDLQManagerConfigFromClientConfig - создаёт конфиг DLQ менеджера из конфига клиента
// Базовые настройки берутся из clientConfig.Producer.DLQ, остальные - из переданного dlqConfig
// Если dlqConfig == nil, используются значения по умолчанию
func NewDLQManagerConfigFromClientConfig(clientConfig *configs.KafkaClientConfig, dlqConfig *DLQManagerConfig) *DLQManagerConfig {
	if dlqConfig == nil {
		dlqConfig = DefaultDLQManagerConfig()
	}

	// Переопределяем базовые настройки из конфига клиента
	if clientConfig != nil {
		dlqConfig.Enabled = clientConfig.IsDLQEnabled()
		dlqConfig.TopicSuffix = clientConfig.Producer.DLQ.TopicSuffix
		dlqConfig.IncludeErrorHeaders = clientConfig.Producer.DLQ.IncludeErrorHeaders
		dlqConfig.SendTimeout = clientConfig.Producer.DLQ.SendTimeout
		dlqConfig.MaxMessageSize = clientConfig.Producer.DLQ.MaxMessageSize
	}

	return dlqConfig
}

// GetDLQTopic - формирует имя топика для DLQ
func (c *DLQManagerConfig) GetDLQTopic(originalTopic string) string {
	if c.TopicSuffix == "" {
		return originalTopic + ".dlq"
	}
	return originalTopic + c.TopicSuffix
}

// Validate - валидация конфигурации
func (c *DLQManagerConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	// Валидация асинхронных настроек
	if c.AsyncMode {
		if c.QueueSize <= 0 {
			return ErrInvalidConfig
		}
		if c.Workers <= 0 {
			return ErrInvalidConfig
		}
		if c.BatchSize <= 0 {
			return ErrInvalidConfig
		}
		if c.FlushTimeout <= 0 {
			return ErrInvalidConfig
		}
	}

	// Общие валидации
	if c.SendTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.MaxRetries < 0 {
		return ErrInvalidConfig
	}
	if c.RetryBackoff <= 0 {
		return ErrInvalidConfig
	}

	return nil
}
