package configs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// buildDSN собирает DSN строку из компонентов
func buildDSN(host, port, user, password, dbName, sslMode string) string {
	parts := []string{
		"host=" + host,
		"port=" + port,
		"user=" + user,
		"password=" + password,
		"dbname=" + dbName,
		"sslmode=" + sslMode,
	}
	return strings.Join(parts, " ")
}

// getRequiredEnv получает обязательную переменную окружения
func getRequiredEnv(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return val, nil
}

// getEnvWithDefault получает переменную окружения или значение по умолчанию
func getEnvWithDefault(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

// getEnvAsInt32WithValidation получает переменную окружения как int32 с валидацией
func getEnvAsInt32WithValidation(key string, defaultValue, min, max int32) (int32, error) {
	if val := os.Getenv(key); val != "" {
		// Atoi возвращает int, что на большинстве систем = int64
		i, err := strconv.Atoi(val)
		if err != nil {
			return defaultValue, fmt.Errorf("%s: must be an integer, got %q", key, val)
		}

		result := int32(i)

		// Проверяем, что значение помещается в int32
		if int64(i) != int64(result) {
			return defaultValue, fmt.Errorf("%s: value %d is too large for int32", key, i)
		}

		if result < min || result > max {
			return defaultValue, fmt.Errorf("%s: value %d is out of range [%d, %d]", key, result, min, max)
		}

		return result, nil
	}
	return defaultValue, nil
}

// getEnvAsDurationWithValidation получает переменную окружения как time.Duration с валидацией
func getEnvAsDurationWithValidation(key string, defaultValue, min, max time.Duration) (time.Duration, error) {
	if val := os.Getenv(key); val != "" {
		// Пробуем распарсить как duration строку
		d, err := time.ParseDuration(val)
		if err != nil {
			// Пробуем как число (предполагаем секунды)
			i, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return defaultValue, fmt.Errorf("%s: must be a duration (like '1m', '1h') or number of seconds, got %q", key, val)
			}
			d = time.Duration(i) * time.Second
		}

		if d < min || d > max {
			return defaultValue, fmt.Errorf("%s: duration %v is out of range [%v, %v]", key, d, min, max)
		}

		return d, nil
	}
	return defaultValue, nil
}
