package configs

import (
	"os"
	"testing"
	"time"
)

// тест для вспомогательной функции которая собирает DSN строку из компонентов
func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     string
		user     string
		password string
		dbName   string
		sslMode  string
		expected string
	}{
		{
			name:     "standard case",
			host:     "localhost",
			port:     "5432",
			user:     "postgres",
			password: "secret",
			dbName:   "mydb",
			sslMode:  "disable",
			expected: "host=localhost port=5432 user=postgres password=secret dbname=mydb sslmode=disable",
		},
		{
			name:     "empty password",
			host:     "localhost",
			port:     "5432",
			user:     "postgres",
			password: "",
			dbName:   "mydb",
			sslMode:  "require",
			expected: "host=localhost port=5432 user=postgres password= dbname=mydb sslmode=require",
		},
		{
			name:     "all fields minimal",
			host:     "db",
			port:     "5432",
			user:     "user",
			password: "pass",
			dbName:   "test",
			sslMode:  "verify-full",
			expected: "host=db port=5432 user=user password=pass dbname=test sslmode=verify-full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildDSN(tt.host, tt.port, tt.user, tt.password, tt.dbName, tt.sslMode)
			if result != tt.expected {
				t.Errorf("buildDSN() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// тест для вспомогательной функции которая получает обязательную переменную окружения
func TestGetRequiredEnv(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		setupEnv  func()
		cleanup   func()
		wantValue string
		wantError bool
	}{
		{
			name: "existing variable",
			key:  "DB_HOST",
			setupEnv: func() {
				os.Setenv("DB_HOST", "localhost")
			},
			cleanup: func() {
				os.Unsetenv("DB_HOST")
			},
			wantValue: "localhost",
			wantError: false,
		},
		{
			name: "empty string variable",
			key:  "EMPTY_VAR",
			setupEnv: func() {
				os.Setenv("EMPTY_VAR", "")
			},
			cleanup: func() {
				os.Unsetenv("EMPTY_VAR")
			},
			wantValue: "",
			wantError: true, // пустая строка считается отсутствующей
		},
		{
			name:      "missing variable",
			key:       "NON_EXISTENT_KEY_12345",
			setupEnv:  func() {},
			cleanup:   func() {},
			wantValue: "",
			wantError: true,
		},
		{
			name: "variable with spaces",
			key:  "CONFIG_PATH",
			setupEnv: func() {
				os.Setenv("CONFIG_PATH", "/etc/myapp/config.yaml")
			},
			cleanup: func() {
				os.Unsetenv("CONFIG_PATH")
			},
			wantValue: "/etc/myapp/config.yaml",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			defer tt.cleanup()

			got, err := getRequiredEnv(tt.key)

			if (err != nil) != tt.wantError {
				t.Errorf("getRequiredEnv() error = %v, wantError %v", err, tt.wantError)
				return
			}

			if got != tt.wantValue {
				t.Errorf("getRequiredEnv() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

// тест для вспомогательной функции которая получает переменную окружения или значение по умолчанию
func TestGetEnvWithDefault(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		setupEnv     func()
		cleanup      func()
		expected     string
	}{
		{
			name:         "env variable exists",
			key:          "LOG_LEVEL",
			defaultValue: "info",
			setupEnv: func() {
				os.Setenv("LOG_LEVEL", "debug")
			},
			cleanup: func() {
				os.Unsetenv("LOG_LEVEL")
			},
			expected: "debug",
		},
		{
			name:         "env variable missing - use default",
			key:          "NON_EXISTENT_KEY",
			defaultValue: "default_value",
			setupEnv:     func() {},
			cleanup:      func() {},
			expected:     "default_value",
		},
		{
			name:         "empty env value - use empty string",
			key:          "EMPTY_VAR",
			defaultValue: "",
			setupEnv: func() {
				os.Setenv("EMPTY_VAR", "")
			},
			cleanup: func() {
				os.Unsetenv("EMPTY_VAR")
			},
			expected: "", // пустая строка считается валидным значением
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			defer tt.cleanup()

			result := getEnvWithDefault(tt.key, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("getEnvWithDefault() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// тест для вспомогательной функции которая получает переменную окружения как int32 с валидацией
func TestGetEnvAsInt32WithValidation(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue int32
		min          int32
		max          int32
		setupEnv     func()
		cleanup      func()
		expected     int32
		expectError  bool
	}{
		{
			name:         "valid value in range",
			key:          "PORT",
			defaultValue: 8080,
			min:          1024,
			max:          65535,
			setupEnv: func() {
				os.Setenv("PORT", "3000")
			},
			cleanup: func() {
				os.Unsetenv("PORT")
			},
			expected:    3000,
			expectError: false,
		},
		{
			name:         "value below min",
			key:          "PORT",
			defaultValue: 8080,
			min:          1024,
			max:          65535,
			setupEnv: func() {
				os.Setenv("PORT", "80")
			},
			cleanup: func() {
				os.Unsetenv("PORT")
			},
			expected:    8080, // возвращает defaultValue
			expectError: true,
		},
		{
			name:         "value above max",
			key:          "PORT",
			defaultValue: 8080,
			min:          1024,
			max:          65535,
			setupEnv: func() {
				os.Setenv("PORT", "70000")
			},
			cleanup: func() {
				os.Unsetenv("PORT")
			},
			expected:    8080,
			expectError: true,
		},
		{
			name:         "non-integer value",
			key:          "PORT",
			defaultValue: 8080,
			min:          1024,
			max:          65535,
			setupEnv: func() {
				os.Setenv("PORT", "not_a_number")
			},
			cleanup: func() {
				os.Unsetenv("PORT")
			},
			expected:    8080,
			expectError: true,
		},
		{
			name:         "value too large for int32",
			key:          "BIG_NUMBER",
			defaultValue: 100,
			min:          0,
			max:          1000,
			setupEnv: func() {
				os.Setenv("BIG_NUMBER", "3000000000") // > 2^31-1
			},
			cleanup: func() {
				os.Unsetenv("BIG_NUMBER")
			},
			expected:    100,
			expectError: true,
		},
		{
			name:         "missing env - use default",
			key:          "NON_EXISTENT",
			defaultValue: 42,
			min:          0,
			max:          100,
			setupEnv:     func() {},
			cleanup:      func() {},
			expected:     42,
			expectError:  false,
		},
		{
			name:         "min and max equal",
			key:          "FIXED_VALUE",
			defaultValue: 5,
			min:          5,
			max:          5,
			setupEnv: func() {
				os.Setenv("FIXED_VALUE", "5")
			},
			cleanup: func() {
				os.Unsetenv("FIXED_VALUE")
			},
			expected:    5,
			expectError: false,
		},
		{
			name:         "negative value allowed",
			key:          "OFFSET",
			defaultValue: 0,
			min:          -100,
			max:          100,
			setupEnv: func() {
				os.Setenv("OFFSET", "-50")
			},
			cleanup: func() {
				os.Unsetenv("OFFSET")
			},
			expected:    -50,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			defer tt.cleanup()

			got, err := getEnvAsInt32WithValidation(tt.key, tt.defaultValue, tt.min, tt.max)

			if (err != nil) != tt.expectError {
				t.Errorf("getEnvAsInt32WithValidation() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if got != tt.expected {
				t.Errorf("getEnvAsInt32WithValidation() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// тест для вспомогательной функции которая получает переменную окружения как time.Duration с валидацией
func TestGetEnvAsDurationWithValidation(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue time.Duration
		min          time.Duration
		max          time.Duration
		setupEnv     func()
		cleanup      func()
		expected     time.Duration
		expectError  bool
	}{
		{
			name:         "duration string format",
			key:          "TIMEOUT",
			defaultValue: 30 * time.Second,
			min:          1 * time.Second,
			max:          5 * time.Minute,
			setupEnv: func() {
				os.Setenv("TIMEOUT", "2m")
			},
			cleanup: func() {
				os.Unsetenv("TIMEOUT")
			},
			expected:    2 * time.Minute,
			expectError: false,
		},
		{
			name:         "number as seconds",
			key:          "TIMEOUT",
			defaultValue: 30 * time.Second,
			min:          1 * time.Second,
			max:          5 * time.Minute,
			setupEnv: func() {
				os.Setenv("TIMEOUT", "45")
			},
			cleanup: func() {
				os.Unsetenv("TIMEOUT")
			},
			expected:    45 * time.Second,
			expectError: false,
		},
		{
			name:         "value below min",
			key:          "TIMEOUT",
			defaultValue: 30 * time.Second,
			min:          10 * time.Second,
			max:          5 * time.Minute,
			setupEnv: func() {
				os.Setenv("TIMEOUT", "5s")
			},
			cleanup: func() {
				os.Unsetenv("TIMEOUT")
			},
			expected:    30 * time.Second,
			expectError: true,
		},
		{
			name:         "value above max",
			key:          "TIMEOUT",
			defaultValue: 30 * time.Second,
			min:          1 * time.Second,
			max:          1 * time.Minute,
			setupEnv: func() {
				os.Setenv("TIMEOUT", "2m")
			},
			cleanup: func() {
				os.Unsetenv("TIMEOUT")
			},
			expected:    30 * time.Second,
			expectError: true,
		},
		{
			name:         "invalid duration format",
			key:          "TIMEOUT",
			defaultValue: 10 * time.Second,
			min:          1 * time.Second,
			max:          1 * time.Hour,
			setupEnv: func() {
				os.Setenv("TIMEOUT", "invalid")
			},
			cleanup: func() {
				os.Unsetenv("TIMEOUT")
			},
			expected:    10 * time.Second,
			expectError: true,
		},
		{
			name:         "missing env - use default",
			key:          "NON_EXISTENT_TIMEOUT",
			defaultValue: 5 * time.Second,
			min:          1 * time.Second,
			max:          10 * time.Second,
			setupEnv:     func() {},
			cleanup:      func() {},
			expected:     5 * time.Second,
			expectError:  false,
		},
		{
			name:         "complex duration format",
			key:          "RETRY_DELAY",
			defaultValue: 1 * time.Second,
			min:          100 * time.Millisecond,
			max:          10 * time.Second,
			setupEnv: func() {
				os.Setenv("RETRY_DELAY", "1s500ms")
			},
			cleanup: func() {
				os.Unsetenv("RETRY_DELAY")
			},
			expected:    1500 * time.Millisecond,
			expectError: false,
		},
		{
			name:         "zero value as seconds",
			key:          "DELAY",
			defaultValue: 5 * time.Second,
			min:          0,
			max:          10 * time.Second,
			setupEnv: func() {
				os.Setenv("DELAY", "0")
			},
			cleanup: func() {
				os.Unsetenv("DELAY")
			},
			expected:    0,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			defer tt.cleanup()

			got, err := getEnvAsDurationWithValidation(tt.key, tt.defaultValue, tt.min, tt.max)

			if (err != nil) != tt.expectError {
				t.Errorf("getEnvAsDurationWithValidation() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if got != tt.expected {
				t.Errorf("getEnvAsDurationWithValidation() = %v, want %v", got, tt.expected)
			}
		})
	}
}
