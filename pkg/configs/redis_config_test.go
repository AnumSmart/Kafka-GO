package configs

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// глобальная блокировка для доступа к переменным окружения
var envRedisMutex sync.Mutex

// TestEnv - структура для управления тестовым окружением
type TestRedisEnv struct {
	variables map[string]string
}

// NewTestEnv создает новое тестовое окружение
func NewTestRedisEnv() *TestRedisEnv {
	return &TestRedisEnv{
		variables: make(map[string]string),
	}
}

// Cleanup восстанавливает чистое состояние
func (te *TestRedisEnv) Cleanup() {
	envRedisMutex.Lock()
	defer envRedisMutex.Unlock()

	// Удаляем только те переменные, которые устанавливали
	for key := range te.variables {
		os.Unsetenv(key)
	}
}

// Set устанавливает переменную окружения для теста
func (te *TestRedisEnv) Set(key, value string) {
	te.variables[key] = value
}

// Apply применяет все установленные переменные к окружению
func (te *TestRedisEnv) Apply() {
	envRedisMutex.Lock()
	defer envRedisMutex.Unlock()

	for key, value := range te.variables {
		os.Setenv(key, value)
	}
}

// Unset удаляет переменную окружения
func (te *TestRedisEnv) Unset(key string) {
	os.Unsetenv(key)
}

// ========== Helper функции ==========

// ClearAll очищает все переменные БД (используется перед тестом)
func ClearAllRedisEnvVars() {
	envRedisMutex.Lock()
	defer envRedisMutex.Unlock()

	envVars := []string{
		"REDIS_CACHE_HOST", "REDIS_CACHE_PORT", "REDIS_CACHE_PASSWORD", "REDIS_CACHE_DB",
		"REDIS_CACHE_POOL_SIZE", "REDIS_CACHE_MIN_IDLE_CONNS", "REDIS_CACHE_DIAL_TIMEOUT",
		"REDIS_CACHE_READ_TIMEOUT", "REDIS_CACHE_WRITE_TIMEOUT", "REDIS_CACHE_IDLE_TIMEOUT",
		"REDIS_CACHE_POOL_TIMEOUT", "REDIS_CACHE_MAX_CON_AGE", "REDIS_CACHE_MAX_RETRIES",
		"REDIS_CACHE_MIN_RETRY_BACKOFF_MS", "REDIS_CACHE_MAX_RETRY_BACKOFF_MS",
	}
	for _, env := range envVars {
		os.Unsetenv(env)
	}
}

// containsString проверяет вхождение подстроки
func containsStringRedis(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestNewRedisConfigFromEnv(t *testing.T) {
	tests := []struct {
		name          string
		setupEnv      func(*TestRedisEnv)
		expectedError string
		validateFunc  func(*testing.T, *RedisConfig)
	}{
		{
			name: "successful config with defaults",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
			},
			validateFunc: func(t *testing.T, cfg *RedisConfig) {
				if cfg.Host != "localhost" {
					t.Errorf("Host = %s, want localhost", cfg.Host)
				}
				if cfg.Port != "6379" {
					t.Errorf("Port = %s, want 6379", cfg.Port)
				}
				if cfg.Password != "secret" {
					t.Errorf("Password = %s, want secret", cfg.Password)
				}
				if cfg.DB != 0 {
					t.Errorf("DB = %d, want 0", cfg.DB)
				}
				if cfg.PoolSize != 100 {
					t.Errorf("PoolSize = %d, want 100", cfg.PoolSize)
				}
				if cfg.MinIdleConns != 20 {
					t.Errorf("MinIdleConns = %d, want 20", cfg.MinIdleConns)
				}
				if cfg.MaxRetries != 2 {
					t.Errorf("MaxRetries = %d, want 2", cfg.MaxRetries)
				}
				if cfg.DialTimeout != 5*time.Second {
					t.Errorf("DialTimeout = %v, want 5s", cfg.DialTimeout)
				}
				if cfg.ReadTimeout != 3*time.Second {
					t.Errorf("ReadTimeout = %v, want 3s", cfg.ReadTimeout)
				}
				if cfg.WriteTimeout != 3*time.Second {
					t.Errorf("WriteTimeout = %v, want 3s", cfg.WriteTimeout)
				}
				if cfg.IdleTimeout != 5*time.Minute {
					t.Errorf("IdleTimeout = %v, want 5m", cfg.IdleTimeout)
				}
				if cfg.PoolTimeout != 4*time.Minute {
					t.Errorf("PoolTimeout = %v, want 4m", cfg.PoolTimeout)
				}
				if cfg.MaxConnAge != 30*time.Minute {
					t.Errorf("MaxConnAge = %v, want 30m", cfg.MaxConnAge)
				}
				if cfg.MinRetryBackOff != 100*time.Millisecond {
					t.Errorf("MinRetryBackOff = %v, want 100ms", cfg.MinRetryBackOff)
				}
				if cfg.MaxRetryBackOff != 1000*time.Millisecond {
					t.Errorf("MaxRetryBackOff = %v, want 1000ms", cfg.MaxRetryBackOff)
				}
			},
		},
		{
			name: "successful config with custom values",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "redis.example.com")
				env.Set("REDIS_CACHE_PORT", "6380")
				env.Set("REDIS_CACHE_PASSWORD", "strongpass")
				env.Set("REDIS_CACHE_DB", "5")
				env.Set("REDIS_CACHE_POOL_SIZE", "200")
				env.Set("REDIS_CACHE_MIN_IDLE_CONNS", "50")
				env.Set("REDIS_CACHE_MAX_RETRIES", "3")
				env.Set("REDIS_CACHE_DIAL_TIMEOUT", "10s")
				env.Set("REDIS_CACHE_READ_TIMEOUT", "5s")
				env.Set("REDIS_CACHE_WRITE_TIMEOUT", "5s")
				env.Set("REDIS_CACHE_IDLE_TIMEOUT", "10m")
				env.Set("REDIS_CACHE_POOL_TIMEOUT", "5m")
				env.Set("REDIS_CACHE_MAX_CON_AGE", "40m")
				env.Set("REDIS_CACHE_MIN_RETRY_BACKOFF_MS", "200ms")
				env.Set("REDIS_CACHE_MAX_RETRY_BACKOFF_MS", "1500ms")
			},
			validateFunc: func(t *testing.T, cfg *RedisConfig) {
				if cfg.Host != "redis.example.com" {
					t.Errorf("Host = %s, want redis.example.com", cfg.Host)
				}
				if cfg.Port != "6380" {
					t.Errorf("Port = %s, want 6380", cfg.Port)
				}
				if cfg.Password != "strongpass" {
					t.Errorf("Password = %s, want strongpass", cfg.Password)
				}
				if cfg.DB != 5 {
					t.Errorf("DB = %d, want 5", cfg.DB)
				}
				if cfg.PoolSize != 200 {
					t.Errorf("PoolSize = %d, want 200", cfg.PoolSize)
				}
				if cfg.MinIdleConns != 50 {
					t.Errorf("MinIdleConns = %d, want 50", cfg.MinIdleConns)
				}
				if cfg.MaxRetries != 3 {
					t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
				}
				if cfg.DialTimeout != 10*time.Second {
					t.Errorf("DialTimeout = %v, want 10s", cfg.DialTimeout)
				}
				if cfg.ReadTimeout != 5*time.Second {
					t.Errorf("ReadTimeout = %v, want 5s", cfg.ReadTimeout)
				}
				if cfg.WriteTimeout != 5*time.Second {
					t.Errorf("WriteTimeout = %v, want 5s", cfg.WriteTimeout)
				}
				if cfg.IdleTimeout != 10*time.Minute {
					t.Errorf("IdleTimeout = %v, want 10m", cfg.IdleTimeout)
				}
				if cfg.PoolTimeout != 5*time.Minute {
					t.Errorf("PoolTimeout = %v, want 5m", cfg.PoolTimeout)
				}
				if cfg.MaxConnAge != 40*time.Minute {
					t.Errorf("MaxConnAge = %v, want 40m", cfg.MaxConnAge)
				}
				if cfg.MinRetryBackOff != 200*time.Millisecond {
					t.Errorf("MinRetryBackOff = %v, want 200ms", cfg.MinRetryBackOff)
				}
				if cfg.MaxRetryBackOff != 1500*time.Millisecond {
					t.Errorf("MaxRetryBackOff = %v, want 1500ms", cfg.MaxRetryBackOff)
				}
			},
		},
		{
			name: "missing required REDIS_HOST",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
			},
			expectedError: "missing required environment variables",
		},
		{
			name: "missing required REDIS_PORT",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
			},
			expectedError: "missing required environment variables",
		},
		{
			name: "missing required REDIS_PASSWORD",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
			},
			expectedError: "missing required environment variables",
		},
		{
			name: "DB out of range - too high",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_DB", "16")
			},
			expectedError: "out of range",
		},
		{
			name: "DB out of range - negative",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_DB", "-1")
			},
			expectedError: "out of range",
		},
		{
			name: "PoolSize below minimum",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_POOL_SIZE", "0")
			},
			expectedError: "out of range",
		},
		{
			name: "PoolSize above maximum",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_POOL_SIZE", "1001")
			},
			expectedError: "out of range",
		},
		{
			name: "MinIdleConns below minimum",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_MIN_IDLE_CONNS", "0")
			},
			expectedError: "out of range",
		},
		{
			name: "MinIdleConns greater than PoolSize",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_POOL_SIZE", "50")
				env.Set("REDIS_CACHE_MIN_IDLE_CONNS", "100")
			},
			expectedError: "cannot be greater than",
		},
		{
			name: "MaxRetries below minimum",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_MAX_RETRIES", "-1")
			},
			expectedError: "out of range",
		},
		{
			name: "MaxRetries above maximum",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_MAX_RETRIES", "4")
			},
			expectedError: "out of range",
		},
		{
			name: "DialTimeout too short",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_DIAL_TIMEOUT", "500ms")
			},
			expectedError: "out of range",
		},
		{
			name: "DialTimeout too long",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_DIAL_TIMEOUT", "31s")
			},
			expectedError: "out of range",
		},
		{
			name: "Invalid duration format",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_HOST", "localhost")
				env.Set("REDIS_CACHE_PORT", "6379")
				env.Set("REDIS_CACHE_PASSWORD", "secret")
				env.Set("REDIS_CACHE_READ_TIMEOUT", "invalid")
			},
			expectedError: "must be a duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEnv := NewTestRedisEnv()
			defer testEnv.Cleanup()

			ClearAllRedisEnvVars()

			if tt.setupEnv != nil {
				tt.setupEnv(testEnv)
			}
			testEnv.Apply()

			config, err := NewRedisConfigFromEnv("REDIS_CACHE")

			if tt.expectedError != "" {
				if err == nil {
					t.Errorf("Expected error containing '%s', got nil", tt.expectedError)
				} else if !containsStringRedis(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.expectedError, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if config == nil {
				t.Errorf("Expected config, got nil")
				return
			}

			if tt.validateFunc != nil {
				tt.validateFunc(t, config)
			}
		})
	}
}

// TestRedisConfigBoundaryValues тестирует граничные значения
func TestRedisConfigBoundaryValues(t *testing.T) {
	tests := []struct {
		name       string
		setupEnv   func(*TestRedisEnv)
		shouldFail bool
	}{
		{
			name: "DB = 0 (minimum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_DB", "0")
			},
			shouldFail: false,
		},
		{
			name: "DB = 15 (maximum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_DB", "15")
			},
			shouldFail: false,
		},
		{
			name: "PoolSize = 1 (minimum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_POOL_SIZE", "1")
			},
			shouldFail: true, // нижняя граница = 1
		},
		{
			name: "PoolSize = 1000 (maximum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_POOL_SIZE", "1000")
			},
			shouldFail: false,
		},
		{
			name: "MinIdleConns = 1 (minimum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_MIN_IDLE_CONNS", "1")
			},
			shouldFail: false,
		},
		{
			name: "MinIdleConns = 1000 (maximum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_MIN_IDLE_CONNS", "1000")
			},
			shouldFail: true, // верхняя граница = 1000
		},
		{
			name: "DialTimeout = 1s (minimum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_DIAL_TIMEOUT", "1s")
			},
			shouldFail: false,
		},
		{
			name: "DialTimeout = 30s (maximum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_DIAL_TIMEOUT", "30s")
			},
			shouldFail: false,
		},
		{
			name: "MinRetryBackoff = 50ms (minimum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_MIN_RETRY_BACKOFF_MS", "50ms")
			},
			shouldFail: false,
		},
		{
			name: "MinRetryBackoff = 300ms (maximum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_MIN_RETRY_BACKOFF_MS", "300ms")
			},
			shouldFail: false,
		},
		{
			name: "MaxRetryBackoff = 512ms (minimum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_MAX_RETRY_BACKOFF_MS", "512ms")
			},
			shouldFail: false,
		},
		{
			name: "MaxRetryBackoff = 2000ms (maximum)",
			setupEnv: func(env *TestRedisEnv) {
				env.Set("REDIS_CACHE_MAX_RETRY_BACKOFF_MS", "2000ms")
			},
			shouldFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEnv := NewTestRedisEnv()
			defer testEnv.Cleanup()

			ClearAllRedisEnvVars()

			// Базовые обязательные поля
			testEnv.Set("REDIS_CACHE_HOST", "localhost")
			testEnv.Set("REDIS_CACHE_PORT", "6379")
			testEnv.Set("REDIS_CACHE_PASSWORD", "secret")

			if tt.setupEnv != nil {
				tt.setupEnv(testEnv)
			}
			testEnv.Apply()

			config, err := NewRedisConfigFromEnv("REDIS_CACHE")

			if tt.shouldFail && err == nil {
				t.Errorf("Expected error but got none")
			}

			if !tt.shouldFail && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.shouldFail && config == nil {
				t.Errorf("Expected config but got nil")
			}
		})
	}
}

// TestRedisConfigToRedisOptions тестирует преобразование конфига в redis.Options
func TestRedisConfigToRedisOptions(t *testing.T) {
	tests := []struct {
		name   string
		config *RedisConfig
		verify func(*testing.T, *redis.Options)
	}{
		{
			name: "basic config",
			config: &RedisConfig{
				Host:            "localhost",
				Port:            "6379",
				Password:        "secret",
				DB:              0,
				PoolSize:        100,
				MinIdleConns:    20,
				MaxRetries:      2,
				DialTimeout:     5 * time.Second,
				ReadTimeout:     3 * time.Second,
				WriteTimeout:    3 * time.Second,
				IdleTimeout:     5 * time.Minute,
				PoolTimeout:     4 * time.Minute,
				MaxConnAge:      30 * time.Minute,
				MinRetryBackOff: 100 * time.Millisecond,
				MaxRetryBackOff: 1000 * time.Millisecond,
			},
			verify: func(t *testing.T, opts *redis.Options) {
				if opts.Addr != "localhost:6379" {
					t.Errorf("Addr = %s, want localhost:6379", opts.Addr)
				}
				if opts.Password != "secret" {
					t.Errorf("Password = %s, want secret", opts.Password)
				}
				if opts.DB != 0 {
					t.Errorf("DB = %d, want 0", opts.DB)
				}
				if opts.PoolSize != 100 {
					t.Errorf("PoolSize = %d, want 100", opts.PoolSize)
				}
				if opts.MinIdleConns != 20 {
					t.Errorf("MinIdleConns = %d, want 20", opts.MinIdleConns)
				}
				if opts.MaxRetries != 2 {
					t.Errorf("MaxRetries = %d, want 2", opts.MaxRetries)
				}
				if opts.DialTimeout != 5*time.Second {
					t.Errorf("DialTimeout = %v, want 5s", opts.DialTimeout)
				}
				if opts.ReadTimeout != 3*time.Second {
					t.Errorf("ReadTimeout = %v, want 3s", opts.ReadTimeout)
				}
				if opts.WriteTimeout != 3*time.Second {
					t.Errorf("WriteTimeout = %v, want 3s", opts.WriteTimeout)
				}
				if opts.IdleTimeout != 5*time.Minute {
					t.Errorf("IdleTimeout = %v, want 5m", opts.IdleTimeout)
				}
				if opts.PoolTimeout != 4*time.Minute {
					t.Errorf("PoolTimeout = %v, want 4m", opts.PoolTimeout)
				}
				if opts.MaxConnAge != 30*time.Minute {
					t.Errorf("MaxConnAge = %v, want 30m", opts.MaxConnAge)
				}
				if opts.MinRetryBackoff != 100*time.Millisecond {
					t.Errorf("MinRetryBackoff = %v, want 100ms", opts.MinRetryBackoff)
				}
				if opts.MaxRetryBackoff != 1000*time.Millisecond {
					t.Errorf("MaxRetryBackoff = %v, want 1000ms", opts.MaxRetryBackoff)
				}
			},
		},
		{
			name: "config with custom values",
			config: &RedisConfig{
				Host:            "redis.prod.com",
				Port:            "7000",
				Password:        "prod_pass",
				DB:              5,
				PoolSize:        250,
				MinIdleConns:    50,
				MaxRetries:      3,
				DialTimeout:     10 * time.Second,
				ReadTimeout:     8 * time.Second,
				WriteTimeout:    8 * time.Second,
				IdleTimeout:     15 * time.Minute,
				PoolTimeout:     6 * time.Minute,
				MaxConnAge:      45 * time.Minute,
				MinRetryBackOff: 200 * time.Millisecond,
				MaxRetryBackOff: 1500 * time.Millisecond,
			},
			verify: func(t *testing.T, opts *redis.Options) {
				if opts.Addr != "redis.prod.com:7000" {
					t.Errorf("Addr = %s, want redis.prod.com:7000", opts.Addr)
				}
				if opts.Password != "prod_pass" {
					t.Errorf("Password = %s, want prod_pass", opts.Password)
				}
				if opts.DB != 5 {
					t.Errorf("DB = %d, want 5", opts.DB)
				}
				if opts.PoolSize != 250 {
					t.Errorf("PoolSize = %d, want 250", opts.PoolSize)
				}
				if opts.MaxRetries != 3 {
					t.Errorf("MaxRetries = %d, want 3", opts.MaxRetries)
				}
				if opts.MinRetryBackoff != 200*time.Millisecond {
					t.Errorf("MinRetryBackoff = %v, want 200ms", opts.MinRetryBackoff)
				}
			},
		},
		{
			name: "zero values",
			config: &RedisConfig{
				Host:            "",
				Port:            "",
				Password:        "",
				DB:              0,
				PoolSize:        0,
				MinIdleConns:    0,
				MaxRetries:      0,
				DialTimeout:     0,
				ReadTimeout:     0,
				WriteTimeout:    0,
				IdleTimeout:     0,
				PoolTimeout:     0,
				MaxConnAge:      0,
				MinRetryBackOff: 0,
				MaxRetryBackOff: 0,
			},
			verify: func(t *testing.T, opts *redis.Options) {
				if opts.Addr != ":" {
					t.Errorf("Addr = %s, want :", opts.Addr)
				}
				if opts.PoolSize != 0 {
					t.Errorf("PoolSize = %d, want 0", opts.PoolSize)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.config.ToRedisOptions()
			if opts == nil {
				t.Errorf("ToRedisOptions returned nil")
				return
			}
			tt.verify(t, opts)
		})
	}
}

// TestRedisConfigDurationFormats тестирует различные форматы duration
func TestRedisConfigDurationFormats(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected time.Duration
		field    string
	}{
		{"seconds as number", "30", 30 * time.Second, "REDIS_CACHE_DIAL_TIMEOUT"},
		{"seconds with s", "30s", 30 * time.Second, "REDIS_CACHE_DIAL_TIMEOUT"},
		{"minutes", "5m", 5 * time.Minute, "REDIS_CACHE_IDLE_TIMEOUT"},
		{"hours", "2h", 2 * time.Hour, "REDIS_CACHE_IDLE_TIMEOUT"},
		{"mixed", "1h03m", 63 * time.Minute, "REDIS_CACHE_MAX_CON_AGE"},
		{"milliseconds", "500ms", 500 * time.Millisecond, "REDIS_CACHE_MIN_RETRY_BACKOFF_MS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEnv := NewTestRedisEnv()
			defer testEnv.Cleanup()

			ClearAllRedisEnvVars()

			testEnv.Set("REDIS_CACHE_HOST", "localhost")
			testEnv.Set("REDIS_CACHE_PORT", "6379")
			testEnv.Set("REDIS_CACHE_PASSWORD", "secret")
			testEnv.Set(tt.field, tt.value)
			testEnv.Apply()

			config, err := NewRedisConfigFromEnv("REDIS_CACHE")
			if err != nil {
				t.Errorf("Failed to parse %s=%s: %v", tt.field, tt.value, err)
				return
			}

			var actual time.Duration
			switch tt.field {
			case "REDIS_CACHE_DIAL_TIMEOUT":
				actual = config.DialTimeout
			case "REDIS_CACHE_READ_TIMEOUT":
				actual = config.ReadTimeout
			case "REDIS_CACHE_WRITE_TIMEOUT":
				actual = config.WriteTimeout
			case "REDIS_CACHE_IDLE_TIMEOUT":
				actual = config.IdleTimeout
			case "REDIS_CACHE_POOL_TIMEOUT":
				actual = config.PoolTimeout
			case "REDIS_CACHE_MAX_CON_AGE":
				actual = config.MaxConnAge
			case "REDIS_CACHE_MIN_RETRY_BACKOFF_MS":
				actual = config.MinRetryBackOff
			case "REDIS_CACHE_MAX_RETRY_BACKOFF_MS":
				actual = config.MaxRetryBackOff
			}

			if actual != tt.expected {
				fmt.Printf("%s = %v, want %v", tt.field, actual, tt.expected)
				t.Errorf("%s = %v, want %v", tt.field, actual, tt.expected)
			}
		})
	}
}
