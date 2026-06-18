package logger

// LoggerConfig - конфигурация логгера
type LoggerConfig struct {
	Level     string `yaml:"level" json:"level"`           // debug, info, warn, error
	Format    string `yaml:"format" json:"format"`         // json, text
	AddSource bool   `yaml:"add_source" json:"add_source"` // добавлять файл:строку
	Service   string `yaml:"service" json:"service"`       // имя микросервиса
}

// DefaultLoggerConfig - дефолтная конфигурация логгера
func DefaultLoggerConfig() *LoggerConfig {
	return &LoggerConfig{
		Level:     "info",
		Format:    "json",
		AddSource: true,
		Service:   "unknown",
	}
}
