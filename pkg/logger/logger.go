package logger

import (
	"log/slog"
	"os"
	"sync"
)

var (
	globalLogger *slog.Logger
	once         sync.Once
)

// InitLogger - инициализация глобального логгера для микросервиса
// Вызывается один раз при старте каждого микросервиса
func InitLogger(cfg *LoggerConfig) {
	once.Do(func() {
		globalLogger = NewLoggerFromConfig(cfg)
	})
}

// GetLogger - возвращает глобальный логгер микросервиса
func GetLogger() *slog.Logger {
	if globalLogger == nil {
		// Автоматическая инициализация с дефолтными настройками
		once.Do(func() {
			globalLogger = NewLoggerFromConfig(nil)
		})
	}
	return globalLogger
}

// NewLoggerFromConfig - создает новый экземпляр логгера
// Каждый микросервис создает свой экземпляр с своей конфигурацией
func NewLoggerFromConfig(cfg *LoggerConfig) *slog.Logger {
	if cfg == nil {
		cfg = DefaultLoggerConfig()
	}

	level := parseLevel(cfg.Level)

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: cfg.AddSource,
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	// Добавляем service как константное поле для всех логов
	if cfg.Service != "" && cfg.Service != "unknown" {
		handler = handler.WithAttrs([]slog.Attr{
			slog.String("service", cfg.Service),
		})
	}

	return slog.New(handler)
}

// NewLogger - упрощенный конструктор (для локальной разработки и тестов)
func NewLogger(level, format, service string) *slog.Logger {
	return NewLoggerFromConfig(&LoggerConfig{
		Level:     level,
		Format:    format,
		AddSource: true,
		Service:   service,
	})
}

// parseLevel - парсит строковый уровень в slog.Level
func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// MustGetLogger - возвращает логгер или паникует (для обязательной инициализации)
func MustGetLogger() *slog.Logger {
	logger := GetLogger()
	if logger == nil {
		panic("logger is not initialized")
	}
	return logger
}
