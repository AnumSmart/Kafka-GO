package logger

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

// Типы для ключей контекста (чтобы избежать коллизий)
type contextKey string

const (
	// TraceIDKey - ключ для trace_id в контексте
	TraceIDKey contextKey = "trace_id"
	// RequestIDKey - ключ для request_id в контексте
	RequestIDKey contextKey = "request_id"
	// UserIDKey - ключ для user_id в контексте
	UserIDKey contextKey = "user_id"
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
// Всегда возвращает валидный логгер (автоматическая инициализация с дефолтными настройками)
func GetLogger() *slog.Logger {
	once.Do(func() {
		if globalLogger == nil {
			globalLogger = NewLoggerFromConfig(nil)
		}
	})
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

// ===================== ФУНКЦИИ С КОНТЕКСТОМ (всегда возвращают валидный логгер) =====================

// WithContext - возвращает логгер с атрибутами из контекста
// Всегда возвращает валидный логгер (авто-инициализация если логгер не создан)
// Поддерживает trace_id, request_id, user_id и другие кастомные поля
func WithContext(ctx context.Context) *slog.Logger {
	logger := GetLogger() // всегда валидный

	attrs := extractContextAttrs(ctx)
	if len(attrs) == 0 {
		return logger
	}

	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}

	return logger.With(args...)
}

// WithContextAndLogger - принимает существующий логгер и обогащает его атрибутами из контекста
// Если переданный логгер nil, использует глобальный (авто-инициализация)
// Всегда возвращает валидный логгер
func WithContextAndLogger(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = GetLogger() // всегда валидный
	}

	attrs := extractContextAttrs(ctx)
	if len(attrs) == 0 {
		return logger
	}

	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}

	return logger.With(args...)
}

// WithContextOrDefault - возвращает логгер с контекстом или указанный fallback логгер
// Если fallback nil, используется глобальный логгер
// Всегда возвращает валидный логгер
func WithContextOrDefault(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	logger := GetLogger()
	if logger == nil {
		if fallback != nil {
			return fallback
		}
		// Последний шанс - стандартный логгер
		return slog.Default()
	}

	attrs := extractContextAttrs(ctx)
	if len(attrs) == 0 {
		return logger
	}

	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}

	return logger.With(args...)
}

// ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================

// extractContextAttrs - извлекает атрибуты из контекста
func extractContextAttrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}

	var attrs []slog.Attr

	// Извлекаем trace_id
	if traceID := ctx.Value(TraceIDKey); traceID != nil {
		if str, ok := traceID.(string); ok && str != "" {
			attrs = append(attrs, slog.String("trace_id", str))
		}
	}

	// Извлекаем request_id
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		if str, ok := requestID.(string); ok && str != "" {
			attrs = append(attrs, slog.String("request_id", str))
		}
	}

	// Извлекаем user_id
	if userID := ctx.Value(UserIDKey); userID != nil {
		if str, ok := userID.(string); ok && str != "" {
			attrs = append(attrs, slog.String("user_id", str))
		}
	}

	return attrs
}

// WithTraceID - добавляет trace_id в контекст
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// WithRequestID - добавляет request_id в контекст
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDKey, requestID)
}

// WithUserID - добавляет user_id в контекст
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

// GetTraceIDFromContext - извлекает trace_id из контекста
func GetTraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if traceID := ctx.Value(TraceIDKey); traceID != nil {
		if str, ok := traceID.(string); ok {
			return str
		}
	}
	return ""
}

// GetRequestIDFromContext - извлекает request_id из контекста
func GetRequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		if str, ok := requestID.(string); ok {
			return str
		}
	}
	return ""
}

// GetUserIDFromContext - извлекает user_id из контекста
func GetUserIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if userID := ctx.Value(UserIDKey); userID != nil {
		if str, ok := userID.(string); ok {
			return str
		}
	}
	return ""
}
