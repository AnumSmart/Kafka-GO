package franzgoconsumer

import "time"

// ConsumerOptions - настройки consumer (только данные, без логики)
type ConsumerOptions struct {
	// StatsPrintInterval - интервал вывода статистики в лог
	// По умолчанию: 10 секунд
	StatsPrintInterval time.Duration

	// CommitInterval - интервал автоматического коммита оффсетов
	// 0 = ручной коммит (рекомендуется)
	// >0 = авто-коммит каждые N секунд (проще, но можно потерять сообщения)
	CommitInterval time.Duration

	// EnableDebugLog - включить отладочное логирование
	// По умолчанию: false
	EnableDebugLog bool
}

// DefaultOptions - настройки по умолчанию
func DefaultOptions() *ConsumerOptions {
	return &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		CommitInterval:     0, // ручной коммит
		EnableDebugLog:     false,
	}
}

// Validate - валидация настроек
func (o *ConsumerOptions) Validate() error {
	if o.StatsPrintInterval <= 0 {
		o.StatsPrintInterval = 10 * time.Second
	}

	if o.CommitInterval < 0 {
		o.CommitInterval = 0
	}

	return nil
}
