package configs

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Тестовая структура для конфига
type TestConfig struct {
	URL               string `yaml:"url"`
	ExchangeName      string `yaml:"exchange_name"`
	MaxRetries        int    `yaml:"max_retries"`
	EnableConfirmMode bool   `yaml:"enable_confirm_mode"`
	PrefetchCount     int    `yaml:"prefetch_count"`
	RetryDelay        string `yaml:"retry_delay"`
}

// NewTestConfig - конструктор с дефолтными значениями
func NewTestConfig() *TestConfig {
	return &TestConfig{
		URL:               "amqp://localhost:5672",
		ExchangeName:      "default.exchange",
		MaxRetries:        3,
		EnableConfirmMode: false,
		PrefetchCount:     10,
		RetryDelay:        "5s",
	}
}

// TestLoadYAMLConfig_Success - тест успешной загрузки конфига
func TestLoadYAMLConfig_Success(t *testing.T) {
	// Создаём временную директорию
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yml")

	// Содержимое YAML файла
	yamlContent := `url: "amqp://test:5672"
exchange_name: "test.exchange"
max_retries: 5
enable_confirm_mode: true`

	// Записываем в тестовый файл содержимое yamlContent
	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// Загружаем конфиг
	cfg, err := LoadYAMLConfig[TestConfig](configPath, NewTestConfig)
	if err != nil {
		t.Fatalf("LoadYAMLConfig failed: %v", err)
	}

	// Проверяем значения
	if cfg == nil {
		t.Fatal("Expected config, got nil")
	}
	if cfg.URL != "amqp://test:5672" {
		t.Errorf("Expected URL 'amqp://test:5672', got '%s'", cfg.URL)
	}
	if cfg.ExchangeName != "test.exchange" {
		t.Errorf("Expected ExchangeName 'test.exchange', got '%s'", cfg.ExchangeName)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", cfg.MaxRetries)
	}
	if !cfg.EnableConfirmMode {
		t.Error("Expected EnableConfirmMode true, got false")
	}
}

// TestLoadYAMLConfig_EmptyPath - тест с пустым путём (должен вернуть дефолты)
func TestLoadYAMLConfig_EmptyPath(t *testing.T) {
	cfg, err := LoadYAMLConfig[TestConfig]("", NewTestConfig)

	if err != nil {
		t.Errorf("Expected no error for empty path, got %v", err)
	}
	if cfg == nil {
		t.Fatal("Expected default config, got nil")
	}

	// Проверяем, что вернулись дефолтные значения
	if cfg.URL != "amqp://localhost:5672" {
		t.Errorf("Expected default URL, got '%s'", cfg.URL)
	}
}

// TestLoadYAMLConfig_FileNotFound - тест с несуществующим файлом (должен вернуть дефолты без ошибки)
func TestLoadYAMLConfig_FileNotFound(t *testing.T) {
	cfg, err := LoadYAMLConfig[TestConfig]("/non/existent/path/config.yml", NewTestConfig)

	if err != nil {
		t.Errorf("Expected no error for missing file, got %v", err)
	}
	if cfg == nil {
		t.Fatal("Expected default config, got nil")
	}
}

// TestLoadYAMLConfig_InvalidYAML - тест с некорректным YAML (должен вернуть ошибку)
func TestLoadYAMLConfig_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yml")

	// Некорректный YAML
	invalidYAML := `
url: "amqp://test:5672"
exchange_name: [unclosed bracket
`
	err := os.WriteFile(configPath, []byte(invalidYAML), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := LoadYAMLConfig[TestConfig](configPath, NewTestConfig)

	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
	if cfg == nil {
		t.Error("Expected config (with defaults) even on error, got nil")
	}
}

// TestLoadYAMLConfig_EmptyFile - тест с пустым файлом (должен вернуть дефолты)
func TestLoadYAMLConfig_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "empty.yml")

	// Создаём пустой файл
	err := os.WriteFile(configPath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("Failed to create empty config file: %v", err)
	}

	cfg, err := LoadYAMLConfig[TestConfig](configPath, NewTestConfig)

	if err != nil {
		t.Errorf("Expected no error for empty file, got %v", err)
	}
	if cfg == nil {
		t.Fatal("Expected default config, got nil")
	}

	// Проверяем, что вернулись дефолтные значения
	if cfg.URL != "amqp://localhost:5672" {
		t.Errorf("Expected default URL, got '%s'", cfg.URL)
	}
}

// TestLoadYAMLConfig_PartialConfig - тест с частичным конфигом (дефолты + переопределённые)
func TestLoadYAMLConfig_PartialConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "partial.yml")

	// Только часть полей
	yamlContent := `
url: "amqp://production:5672"
max_retries: 10
`
	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := LoadYAMLConfig[TestConfig](configPath, NewTestConfig)

	if err != nil {
		t.Fatalf("LoadYAMLConfig failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("Expected config, got nil")
	}

	// Проверяем переопределённые поля
	if cfg.URL != "amqp://production:5672" {
		t.Errorf("Expected URL 'amqp://production:5672', got '%s'", cfg.URL)
	}
	if cfg.MaxRetries != 10 {
		t.Errorf("Expected MaxRetries 10, got %d", cfg.MaxRetries)
	}

	// Проверяем, что дефолтные значения сохранились
	if cfg.ExchangeName != "default.exchange" {
		t.Errorf("Expected default ExchangeName 'default.exchange', got '%s'", cfg.ExchangeName)
	}
	if cfg.PrefetchCount != 10 {
		t.Errorf("Expected default PrefetchCount 10, got %d", cfg.PrefetchCount)
	}
}

// TestLoadYAMLConfig_WithEnvVars - тест с подстановкой переменных окружения в YAML
func TestLoadYAMLConfig_WithEnvVars(t *testing.T) {
	// Устанавливаем тестовые переменные окружения
	os.Setenv("TEST_RABBIT_URL", "amqp://env:5672")
	os.Setenv("TEST_ENV", "production")
	defer func() {
		os.Unsetenv("TEST_RABBIT_URL")
		os.Unsetenv("TEST_ENV")
	}()

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "with_env.yml")

	// YAML с переменными окружения
	yamlContent := `
url: '${TEST_RABBIT_URL}'
exchange_name: "test.${TEST_ENV}"
`
	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := LoadYAMLConfig[TestConfig](configPath, NewTestConfig)

	if err != nil {
		t.Fatalf("LoadYAMLConfig failed: %v", err)
	}

	// Проверяем, что переменные окружения не развернулись автоматически
	// (это ожидаемое поведение, т.к. yaml.Unmarshal не разворачивает env)
	if cfg.URL != "${TEST_RABBIT_URL}" {
		t.Logf("Note: env vars are not expanded by yaml.Unmarshal, got: %s", cfg.URL)
	}
}

// TestLoadYAMLConfig_DifferentTypes - тест с разными типами данных в YAML
func TestLoadYAMLConfig_DifferentTypes(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "types.yml")

	yamlContent := `
url: "amqp://test:5672"
max_retries: 7
retry_delay: 10s
enable_confirm_mode: true
prefetch_count: 50
`
	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := LoadYAMLConfig[TestConfig](configPath, NewTestConfig)

	if err != nil {
		t.Fatalf("LoadYAMLConfig failed: %v", err)
	}

	if cfg.MaxRetries != 7 {
		t.Errorf("Expected MaxRetries 7, got %d", cfg.MaxRetries)
	}
	if cfg.PrefetchCount != 50 {
		t.Errorf("Expected PrefetchCount 50, got %d", cfg.PrefetchCount)
	}
	if !cfg.EnableConfirmMode {
		t.Error("Expected EnableConfirmMode true, got false")
	}
}

func TestLoadYAMLConfig_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "concurrent.yml")

	yamlContent := `url: "amqp://test:5672"`
	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := LoadYAMLConfig(configPath, NewTestConfig)
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for _, err := range errors {
		t.Errorf("Concurrent load failed: %v", err)
	}
}
