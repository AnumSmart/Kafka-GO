package franzgoconsumer

import "time"

// ConsumerOptions - настройки consumer (только данные, без логики)
type ConsumerOptions struct {
	StatsPrintInterval time.Duration
	CommitInterval     time.Duration
	EnableDebugLog     bool

	// DLQ настройки (только флаги, без создания producer'а)
	DLQEnabled bool
}

// DefaultOptions - настройки по умолчанию
func DefaultOptions() *ConsumerOptions {
	return &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		CommitInterval:     0,
		EnableDebugLog:     false,
		DLQEnabled:         false,
	}
}
