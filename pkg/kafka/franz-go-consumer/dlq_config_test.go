package franzgoconsumer

import (
	"pkg/configs"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Тест 5.3: DefaultDLQManagerConfig - значения по умолчанию
// ============================================================================

func TestDefaultDLQManagerConfig(t *testing.T) {
	cfg := DefaultDLQManagerConfig()

	require.NotNil(t, cfg, "DefaultDLQManagerConfig should never return nil")

	// Базовые настройки
	assert.False(t, cfg.Enabled, "DLQ should be disabled by default")
	assert.Equal(t, ".dlq", cfg.TopicSuffix, "Default topic suffix should be .dlq")
	assert.True(t, cfg.IncludeErrorHeaders, "Should include error headers by default")
	assert.Equal(t, 5*time.Second, cfg.SendTimeout, "Send timeout default should be 5s")
	assert.Equal(t, 1048576, cfg.MaxMessageSize, "Max message size default should be 1MB")

	// Асинхронные настройки
	assert.True(t, cfg.AsyncMode, "Async mode should be enabled by default")
	assert.Equal(t, 1000, cfg.QueueSize, "Default queue size should be 1000")
	assert.Equal(t, 3, cfg.Workers, "Default workers count should be 3")
	assert.Equal(t, 10, cfg.BatchSize, "Default batch size should be 10")

	// Таймауты и ретраи
	assert.Equal(t, 30*time.Second, cfg.FlushTimeout, "Default flush timeout should be 30s")
	assert.Equal(t, 3, cfg.MaxRetries, "Default max retries should be 3")
	assert.Equal(t, 100*time.Millisecond, cfg.RetryBackoff, "Default retry backoff should be 100ms")

	// Fallback
	assert.False(t, cfg.FallbackEnabled, "Fallback should be disabled by default")
	assert.Equal(t, "", cfg.FallbackLogPath, "Fallback log path should be empty by default")

	// Сервис
	assert.Equal(t, "kafka-consumer", cfg.ServiceName, "Default service name should be kafka-consumer")
}

// ============================================================================
// Тест доп.: DefaultDLQManagerConfig возвращает новый экземпляр
// ============================================================================

func TestDefaultDLQManagerConfig_ReturnsNewInstance(t *testing.T) {
	cfg1 := DefaultDLQManagerConfig()
	cfg2 := DefaultDLQManagerConfig()

	// Изменяем первый экземпляр
	cfg1.Enabled = true
	cfg1.QueueSize = 5000
	cfg1.Workers = 10

	// Второй экземпляр не должен измениться
	assert.False(t, cfg2.Enabled, "Second instance should still have default values")
	assert.Equal(t, 1000, cfg2.QueueSize, "Second instance should still have default values")
	assert.Equal(t, 3, cfg2.Workers, "Second instance should still have default values")

	// Это разные объекты в памяти
	assert.NotSame(t, cfg1, cfg2, "Should return different instances")
}

// ============================================================================
// Тест: GetDLQTopic - формирование имени топика
// ============================================================================

func TestGetDLQTopic_WithSuffix(t *testing.T) {
	testCases := []struct {
		name          string
		originalTopic string
		topicSuffix   string
		expected      string
	}{
		{
			name:          "Standard topic with default suffix",
			originalTopic: "orders",
			topicSuffix:   ".dlq",
			expected:      "orders.dlq",
		},
		{
			name:          "Topic with underscores",
			originalTopic: "user_events",
			topicSuffix:   "-failed",
			expected:      "user_events-failed",
		},
		{
			name:          "Topic with hyphens",
			originalTopic: "payment-transactions",
			topicSuffix:   "_dlq",
			expected:      "payment-transactions_dlq",
		},
		{
			name:          "Topic with dots",
			originalTopic: "app.service.events",
			topicSuffix:   ".dlq",
			expected:      "app.service.events.dlq",
		},
		{
			name:          "Long topic name",
			originalTopic: "very_long_topic_name_with_many_parts",
			topicSuffix:   ".dead-letter",
			expected:      "very_long_topic_name_with_many_parts.dead-letter",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				TopicSuffix: tc.topicSuffix,
			}

			result := cfg.GetDLQTopic(tc.originalTopic)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestGetDLQTopic_EmptySuffix(t *testing.T) {
	cfg := &DLQManagerConfig{
		TopicSuffix: "",
	}

	result := cfg.GetDLQTopic("orders")

	// Должен добавить .dlq как fallback
	assert.Equal(t, "orders.dlq", result,
		"Should add .dlq suffix when TopicSuffix is empty")
}

func TestGetDLQTopic_NilConfig(t *testing.T) {
	// Вызов на nil конфиге должен паниковать
	var cfg *DLQManagerConfig

	assert.Panics(t, func() {
		cfg.GetDLQTopic("test")
	}, "Calling GetDLQTopic on nil config should panic")
}

// ============================================================================
// Тест: Validate - валидация DLQManagerConfig
// ============================================================================

func TestValidateDLQConfig_Disabled(t *testing.T) {
	// Если DLQ выключен, валидация всегда успешна
	cfg := &DLQManagerConfig{
		Enabled: false,
		// Остальные поля могут быть невалидными
		QueueSize: -1,
		Workers:   0,
		BatchSize: -5,
	}

	err := cfg.Validate()

	assert.NoError(t, err, "Validation should skip checks when DLQ is disabled")
}

func TestValidateDLQConfig_ValidAsyncConfig(t *testing.T) {
	cfg := &DLQManagerConfig{
		Enabled:      true,
		AsyncMode:    true,
		QueueSize:    1000,
		Workers:      3,
		BatchSize:    10,
		FlushTimeout: 30 * time.Second,
		SendTimeout:  5 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}

	err := cfg.Validate()

	assert.NoError(t, err, "Valid async config should pass validation")
}

func TestValidateDLQConfig_InvalidQueueSize(t *testing.T) {
	testCases := []struct {
		name      string
		queueSize int
	}{
		{"QueueSize is zero", 0},
		{"QueueSize is negative", -10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    tc.queueSize,
				Workers:      3,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			}

			err := cfg.Validate()

			assert.ErrorIs(t, err, ErrInvalidConfig,
				"QueueSize <= 0 should return ErrInvalidConfig")
		})
	}
}

func TestValidateDLQConfig_InvalidWorkers(t *testing.T) {
	testCases := []struct {
		name    string
		workers int
	}{
		{"Workers is zero", 0},
		{"Workers is negative", -5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    1000,
				Workers:      tc.workers,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			}

			err := cfg.Validate()

			assert.ErrorIs(t, err, ErrInvalidConfig,
				"Workers <= 0 should return ErrInvalidConfig")
		})
	}
}

func TestValidateDLQConfig_InvalidBatchSize(t *testing.T) {
	testCases := []struct {
		name      string
		batchSize int
	}{
		{"BatchSize is zero", 0},
		{"BatchSize is negative", -1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    1000,
				Workers:      3,
				BatchSize:    tc.batchSize,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			}

			err := cfg.Validate()

			assert.ErrorIs(t, err, ErrInvalidConfig,
				"BatchSize <= 0 should return ErrInvalidConfig")
		})
	}
}

func TestValidateDLQConfig_InvalidFlushTimeout(t *testing.T) {
	cfg := &DLQManagerConfig{
		Enabled:      true,
		AsyncMode:    true,
		QueueSize:    1000,
		Workers:      3,
		BatchSize:    10,
		FlushTimeout: 0, // zero timeout
		SendTimeout:  5 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}

	err := cfg.Validate()

	assert.ErrorIs(t, err, ErrInvalidConfig,
		"FlushTimeout <= 0 should return ErrInvalidConfig")
}

func TestValidateDLQConfig_InvalidSendTimeout(t *testing.T) {
	testCases := []struct {
		name        string
		sendTimeout time.Duration
	}{
		{"SendTimeout is zero", 0},
		{"SendTimeout is negative", -1 * time.Second},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    1000,
				Workers:      3,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  tc.sendTimeout,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			}

			err := cfg.Validate()

			assert.ErrorIs(t, err, ErrInvalidConfig,
				"SendTimeout <= 0 should return ErrInvalidConfig")
		})
	}
}

func TestValidateDLQConfig_InvalidRetryBackoff(t *testing.T) {
	testCases := []struct {
		name         string
		retryBackoff time.Duration
	}{
		{"RetryBackoff is zero", 0},
		{"RetryBackoff is negative", -100 * time.Millisecond},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    1000,
				Workers:      3,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: tc.retryBackoff,
			}

			err := cfg.Validate()

			assert.ErrorIs(t, err, ErrInvalidConfig,
				"RetryBackoff <= 0 should return ErrInvalidConfig")
		})
	}
}

func TestValidateDLQConfig_ValidMaxRetries(t *testing.T) {
	// MaxRetries может быть 0 (без ретраев)
	cfg := &DLQManagerConfig{
		Enabled:      true,
		AsyncMode:    true,
		QueueSize:    1000,
		Workers:      3,
		BatchSize:    10,
		FlushTimeout: 30 * time.Second,
		SendTimeout:  5 * time.Second,
		MaxRetries:   0, // ноль ретраев - валидно
		RetryBackoff: 100 * time.Millisecond,
	}

	err := cfg.Validate()

	assert.NoError(t, err, "MaxRetries = 0 should be valid")
}

func TestValidateDLQConfig_ValidSyncMode(t *testing.T) {
	// Синхронный режим не требует QueueSize, Workers, BatchSize
	cfg := &DLQManagerConfig{
		Enabled:      true,
		AsyncMode:    false,
		SendTimeout:  5 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}

	err := cfg.Validate()

	assert.NoError(t, err, "Sync mode should not require async-specific fields")
}

// ============================================================================
// Тест доп.: NewDLQManagerConfigFromClientConfig
// ============================================================================

func TestNewDLQManagerConfigFromClientConfig_NilClientConfig(t *testing.T) {
	// Если clientConfig nil, используем переданный dlqConfig
	customConfig := &DLQManagerConfig{
		Enabled:   true,
		QueueSize: 2000,
		Workers:   5,
	}

	result := NewDLQManagerConfigFromClientConfig(nil, customConfig)

	assert.NotNil(t, result)
	assert.True(t, result.Enabled)
	assert.Equal(t, 2000, result.QueueSize)
	assert.Equal(t, 5, result.Workers)
}

func TestNewDLQManagerConfigFromClientConfig_NilBoth(t *testing.T) {
	result := NewDLQManagerConfigFromClientConfig(nil, nil)

	assert.NotNil(t, result, "Should return default config")
	assert.False(t, result.Enabled, "Default should have DLQ disabled")
	assert.Equal(t, 1000, result.QueueSize)
	assert.Equal(t, 3, result.Workers)
	assert.Equal(t, 10, result.BatchSize)
}

func TestNewDLQManagerConfigFromClientConfig_WithClientConfig(t *testing.T) {
	// Создаём мок clientConfig
	clientConfig := &configs.KafkaClientConfig{
		Producer: configs.ProducerConfig{
			DLQ: configs.DLQConfig{
				Enabled:             true,
				TopicSuffix:         "-failed",
				IncludeErrorHeaders: false,
				SendTimeout:         10 * time.Second,
				MaxMessageSize:      2097152, // 2MB
			},
		},
	}

	baseConfig := &DLQManagerConfig{
		QueueSize: 500,
		Workers:   2,
	}

	result := NewDLQManagerConfigFromClientConfig(clientConfig, baseConfig)

	// Проверяем, что значения из clientConfig переопределили базовые
	assert.True(t, result.Enabled)
	assert.Equal(t, "-failed", result.TopicSuffix)
	assert.False(t, result.IncludeErrorHeaders)
	assert.Equal(t, 10*time.Second, result.SendTimeout)
	assert.Equal(t, 2097152, result.MaxMessageSize)

	// Проверяем, что значения из baseConfig сохранились
	assert.Equal(t, 500, result.QueueSize)
	assert.Equal(t, 2, result.Workers)
}

func TestNewDLQManagerConfigFromClientConfig_PartialOverride(t *testing.T) {
	clientConfig := &configs.KafkaClientConfig{
		Producer: configs.ProducerConfig{
			DLQ: configs.DLQConfig{
				Enabled: true,
				// TopicSuffix не задан
			},
		},
	}

	baseConfig := DefaultDLQManagerConfig()
	baseConfig.QueueSize = 3000

	result := NewDLQManagerConfigFromClientConfig(clientConfig, baseConfig)

	// Из clientConfig
	assert.True(t, result.Enabled)

	// Осталось из baseConfig (так как в clientConfig не было)
	assert.Equal(t, 3000, result.QueueSize)
	assert.Equal(t, ".dlq", result.TopicSuffix) // из DefaultDLQManagerConfig
}

// ============================================================================
// Тест: Validate возвращает nil для отключённого DLQ
// ============================================================================

func TestValidateDLQConfig_Disabled_IgnoresAllErrors(t *testing.T) {
	// Даже если все поля невалидны, отключённый DLQ должен проходить валидацию
	cfg := &DLQManagerConfig{
		Enabled:      false,
		QueueSize:    -100,
		Workers:      -10,
		BatchSize:    -5,
		FlushTimeout: -1 * time.Second,
		SendTimeout:  -1 * time.Second,
		MaxRetries:   -3,
		RetryBackoff: -1 * time.Second,
	}

	err := cfg.Validate()

	assert.NoError(t, err, "Disabled DLQ should always be valid")
}
