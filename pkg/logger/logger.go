package logger

import (
	"log/slog"
	"os"
)

// NewLoggerFromConfig - создает логгер из конфига
func NewLoggerFromConfig(cfg *LoggerConfig) *slog.Logger {
	if cfg == nil {
		cfg = DefaultLoggerConfig()
	}

	// Уровень логирования
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

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

	return slog.New(handler)
}

// NewLogger - упрощенный конструктор (для локальной разработки)
func NewLogger(level, format string) *slog.Logger {
	return NewLoggerFromConfig(&LoggerConfig{
		Level:     level,
		Format:    format,
		AddSource: true,
	})
}
