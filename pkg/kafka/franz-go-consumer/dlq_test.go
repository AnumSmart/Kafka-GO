package franzgoconsumer

import (
	"context"
	"errors"
	"pkg/configs"
	"pkg/kafka"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestNewDLQManagerConfigFromClientConfig - тест создания конфига из клиентского конфига
func TestNewDLQManagerConfigFromClientConfig(t *testing.T) {
	tests := []struct {
		name         string
		clientConfig *configs.KafkaClientConfig
		dlqConfig    *DLQManagerConfig
		expected     *DLQManagerConfig
	}{
		{
			name:         "nil client config uses defaults",
			clientConfig: nil,
			dlqConfig:    nil,
			expected:     DefaultDLQManagerConfig(),
		},
		{
			name: "client config overrides defaults",
			clientConfig: &configs.KafkaClientConfig{
				Producer: configs.ProducerConfig{
					DLQ: configs.DLQConfig{
						Enabled:             true,
						TopicSuffix:         ".dead-letter",
						IncludeErrorHeaders: false,
						SendTimeout:         10 * time.Second,
						MaxMessageSize:      2048576,
					},
				},
			},
			dlqConfig: nil,
			expected: func() *DLQManagerConfig {
				cfg := DefaultDLQManagerConfig()
				cfg.Enabled = true
				cfg.TopicSuffix = ".dead-letter"
				cfg.IncludeErrorHeaders = false
				cfg.SendTimeout = 10 * time.Second
				cfg.MaxMessageSize = 2048576
				return cfg
			}(),
		},
		{
			name: "custom dlq config with client overrides",
			clientConfig: &configs.KafkaClientConfig{
				Producer: configs.ProducerConfig{
					DLQ: configs.DLQConfig{
						Enabled:     true,
						TopicSuffix: ".client-suffix",
					},
				},
			},
			dlqConfig: &DLQManagerConfig{
				AsyncMode:   false,
				QueueSize:   500,
				Workers:     5,
				BatchSize:   20,
				ServiceName: "custom-service",
			},
			expected: func() *DLQManagerConfig {
				cfg := &DLQManagerConfig{
					Enabled:             true,
					TopicSuffix:         ".client-suffix",
					IncludeErrorHeaders: true,            // из дефолта
					SendTimeout:         5 * time.Second, // из дефолта
					MaxMessageSize:      1048576,         // из дефолта
					AsyncMode:           false,
					QueueSize:           500,
					Workers:             5,
					BatchSize:           20,
					FlushTimeout:        30 * time.Second,
					MaxRetries:          3,
					RetryBackoff:        100 * time.Millisecond,
					FallbackEnabled:     false,
					FallbackLogPath:     "",
					ServiceName:         "custom-service",
				}
				return cfg
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewDLQManagerConfigFromClientConfig(tt.clientConfig, tt.dlqConfig)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDLQManagerConfig_GetDLQTopic - тест формирования имени топика
func TestDLQManagerConfig_GetDLQTopic(t *testing.T) {
	tests := []struct {
		name          string
		topicSuffix   string
		originalTopic string
		expected      string
	}{
		{
			name:          "default suffix",
			topicSuffix:   ".dlq",
			originalTopic: "test-topic",
			expected:      "test-topic.dlq",
		},
		{
			name:          "custom suffix",
			topicSuffix:   ".dead-letter",
			originalTopic: "orders",
			expected:      "orders.dead-letter",
		},
		{
			name:          "empty suffix uses default",
			topicSuffix:   "",
			originalTopic: "user-events",
			expected:      "user-events.dlq",
		},
		{
			name:          "suffix with no dot",
			topicSuffix:   "-dlq",
			originalTopic: "payments",
			expected:      "payments-dlq",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &DLQManagerConfig{
				TopicSuffix: tt.topicSuffix,
			}
			result := cfg.GetDLQTopic(tt.originalTopic)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDLQManagerConfig_Validate - тест валидации конфига
func TestDLQManagerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *DLQManagerConfig
		wantErr error
	}{
		{
			name: "valid async config",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    100,
				Workers:      3,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: nil,
		},
		{
			name: "valid sync config",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    false,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: nil,
		},
		{
			name: "disabled config skips validation",
			config: &DLQManagerConfig{
				Enabled: false,
			},
			wantErr: nil,
		},
		{
			name: "invalid queue size",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    0,
				Workers:      3,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "invalid workers",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    100,
				Workers:      0,
				BatchSize:    10,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "invalid batch size",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    100,
				Workers:      3,
				BatchSize:    0,
				FlushTimeout: 30 * time.Second,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "invalid flush timeout",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				QueueSize:    100,
				Workers:      3,
				BatchSize:    10,
				FlushTimeout: 0,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "invalid send timeout",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    false,
				SendTimeout:  0,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "invalid retry backoff",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    false,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 0,
			},
			wantErr: ErrInvalidConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDLQManager_Send - тест отправки сообщений с новой логикой конфига
func TestDLQManager_Send(t *testing.T) {
	originalMsg := &kafka.Message{
		Topic:     "test-topic",
		Offset:    42,
		Partition: 0,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	tests := []struct {
		name        string
		config      *DLQManagerConfig
		msg         *kafka.Message
		err         error
		expectError bool
	}{
		{
			name: "async mode successful send",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				TopicSuffix:  ".dlq",
				QueueSize:    10,
				Workers:      2,
				BatchSize:    5,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
				FlushTimeout: 30 * time.Second,
			},
			msg:         originalMsg,
			err:         errors.New("test error"),
			expectError: false,
		},
		{
			name: "sync mode successful send",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    false,
				TopicSuffix:  ".dlq",
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
			},
			msg:         originalMsg,
			err:         errors.New("test error"),
			expectError: false,
		},
		{
			name: "DLQ disabled returns no error",
			config: &DLQManagerConfig{
				Enabled: false,
			},
			msg:         originalMsg,
			err:         errors.New("test error"),
			expectError: false,
		},
		{
			name: "async queue full returns error",
			config: &DLQManagerConfig{
				Enabled:      true,
				AsyncMode:    true,
				TopicSuffix:  ".dlq",
				QueueSize:    1,
				Workers:      0, // нет воркеров -> очередь не обрабатывается
				BatchSize:    5,
				SendTimeout:  5 * time.Second,
				MaxRetries:   3,
				RetryBackoff: 100 * time.Millisecond,
				FlushTimeout: 30 * time.Second,
			},
			msg:         originalMsg,
			err:         errors.New("test error"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := NewDLQManager(&kgo.Client{}, "test-topic", tt.config)
			defer dm.Close()

			ctx := context.Background()
			err := dm.Send(ctx, tt.msg, tt.err)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDLQManager_IsEnabled - тест проверки статуса с учётом AsyncMode
func TestDLQManager_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		config   *DLQManagerConfig
		producer *kgo.Client
		closed   bool
		expected bool
	}{
		{
			name: "async fully enabled",
			config: &DLQManagerConfig{
				Enabled:     true,
				AsyncMode:   true,
				TopicSuffix: ".dlq",
			},
			producer: &kgo.Client{},
			closed:   false,
			expected: true,
		},
		{
			name: "sync fully enabled",
			config: &DLQManagerConfig{
				Enabled:     true,
				AsyncMode:   false,
				TopicSuffix: ".dlq",
			},
			producer: &kgo.Client{},
			closed:   false,
			expected: true,
		},
		{
			name: "disabled by config",
			config: &DLQManagerConfig{
				Enabled: false,
			},
			producer: &kgo.Client{},
			closed:   false,
			expected: false,
		},
		{
			name: "closed manager",
			config: &DLQManagerConfig{
				Enabled:     true,
				TopicSuffix: ".dlq",
			},
			producer: &kgo.Client{},
			closed:   true,
			expected: false,
		},
		{
			name: "nil producer",
			config: &DLQManagerConfig{
				Enabled:     true,
				TopicSuffix: ".dlq",
			},
			producer: nil,
			closed:   false,
			expected: false,
		},
		{
			name: "empty topic suffix",
			config: &DLQManagerConfig{
				Enabled:     true,
				TopicSuffix: "",
			},
			producer: &kgo.Client{},
			closed:   false,
			expected: true, // должен использовать ".dlq" как fallback
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := &dlqManager{
				config:   tt.config,
				producer: tt.producer,
				topic:    tt.config.GetDLQTopic("test-topic"),
			}
			if tt.closed {
				dm.closed.Store(true)
			}

			assert.Equal(t, tt.expected, dm.IsEnabled())
		})
	}
}

// TestDLQManager_AsyncVsSyncMode - тест разных режимов работы
func TestDLQManager_AsyncVsSyncMode(t *testing.T) {
	msg := &kafka.Message{
		Topic:  "test-topic",
		Offset: 1,
	}

	t.Run("async mode starts workers", func(t *testing.T) {
		config := &DLQManagerConfig{
			Enabled:      true,
			AsyncMode:    true,
			TopicSuffix:  ".dlq",
			QueueSize:    100,
			Workers:      3,
			BatchSize:    10,
			SendTimeout:  5 * time.Second,
			MaxRetries:   3,
			RetryBackoff: 100 * time.Millisecond,
			FlushTimeout: 30 * time.Second,
		}

		dm := NewDLQManager(&kgo.Client{}, "test-topic", config)
		defer dm.Close()

		// Проверяем, что воркеры запущены
		// Для этого отправляем сообщение и проверяем, что оно ушло в очередь
		ctx := context.Background()
		err := dm.Send(ctx, msg, errors.New("test error"))
		assert.NoError(t, err)
	})

	t.Run("sync mode doesn't start workers", func(t *testing.T) {
		config := &DLQManagerConfig{
			Enabled:      true,
			AsyncMode:    false,
			TopicSuffix:  ".dlq",
			SendTimeout:  5 * time.Second,
			MaxRetries:   3,
			RetryBackoff: 100 * time.Millisecond,
		}

		dm := NewDLQManager(&kgo.Client{}, "test-topic", config)
		defer dm.Close()

		// В синхронном режиме отправка должна работать без очереди
		ctx := context.Background()
		err := dm.Send(ctx, msg, errors.New("test error"))
		assert.NoError(t, err)
	})
}
