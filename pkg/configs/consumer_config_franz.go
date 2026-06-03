package configs

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FranzConsumerConfig - конфигурация консьюмера для franz-go
type FranzConsumerConfig struct {
	// Встраиваем базовый Kafka конфиг
	KafkaConfig `yaml:",inline"`

	// Специфичные настройки консьюмера для franz-go
	// В YAML эти настройки будут в секции "consumer"
	ConsumerSpecific FranzConsumerSpecificConfig `yaml:"consumer" json:"consumer"`
}

// FranzConsumerSpecificConfig - настройки консьюмера для franz-go
type FranzConsumerSpecificConfig struct {
	// Enabled - флаг, включён ли consumer
	Enabled bool `yaml:"enabled" json:"enabled"`

	// StartOffset - с какого смещения начинать чтение
	StartOffset string `yaml:"start_offset" json:"start_offset"`

	// CommitInterval - интервал автоматического коммита
	// 0 = ручной коммит (manual commit) - рекомендуется для надёжности
	// >0 = авто-коммит каждые N секунд - проще, но можно потерять сообщения
	CommitInterval time.Duration `yaml:"commit_interval" json:"commit_interval"`

	// SessionTimeout - таймаут сессии (heartbeat)
	// Если consumer не отправит heartbeat за это время, Kafka считает его мёртвым
	// и инициирует ребалансировку (перераспределение партиций)
	SessionTimeout time.Duration `yaml:"session_timeout" json:"session_timeout"`

	// RebalanceTimeout - таймаут ребалансировки
	// Максимальное время, которое consumer может обрабатывать сообщения
	// до того, как партиции будут отозваны
	RebalanceTimeout time.Duration `yaml:"rebalance_timeout" json:"rebalance_timeout"`

	// MaxRecordsPerFetch - максимальное количество записей за один fetch
	// Ограничивает размер батча, чтобы не перегружать память
	MaxRecordsPerFetch int32 `yaml:"max_records_per_fetch" json:"max_records_per_fetch"`

	// DisableAutoCommit - отключить авто-коммит
	// Если true, коммит происходит только явно через CommitUncommittedOffsets
	// Рекомендуется true для ручного контроля
	DisableAutoCommit bool `yaml:"disable_auto_commit" json:"disable_auto_commit"`
}

// DefaultFranzConsumerConfig - дефолтный конфиг для franz-go консьюмера
func DefaultFranzConsumerConfig() *FranzConsumerConfig {
	return &FranzConsumerConfig{
		// DefaultKafkaConfig определён в другом файле (kafka_config.go)
		KafkaConfig: *DefaultKafkaConfig(),

		// Специфичные настройки consumer
		ConsumerSpecific: FranzConsumerSpecificConfig{
			Enabled:            true,             // consumer активен по умолчанию
			StartOffset:        "latest",         // читаем только новые сообщения
			CommitInterval:     0,                // 0 = используем ручной коммит
			SessionTimeout:     30 * time.Second, // стандартный таймаут сессии
			RebalanceTimeout:   60 * time.Second, // даём минуту на завершение обработки
			MaxRecordsPerFetch: 1000,             // не более 1000 записей за раз
			DisableAutoCommit:  true,             // ручной коммит надёжнее
		},
	}
}

// ToKgoOptions - конвертирует в опции kgo.Client
// Преобразует нашу структуру в слайс опций, понятных библиотеке franz-go
// Возвращает []kgo.Opt - это функциональные опции (variadic parameters pattern)
func (c *FranzConsumerConfig) ToKgoOptions() ([]kgo.Opt, error) {
	// Создаём слайс опций с базовыми настройками
	// kgo.Opt - тип для функциональных опций (каждая опция - это функция)
	opts := []kgo.Opt{
		// SeedBrokers - список брокеров Kafka
		// Принимает переменное количество строк (variadic)
		kgo.SeedBrokers(c.Brokers...),

		// ConsumerGroup - ID группы потребителей
		// Все consumer-ы с одинаковым GroupID образуют группу
		kgo.ConsumerGroup(c.GroupID),

		// ConsumeTopics - список топиков для потребления
		kgo.ConsumeTopics(c.Topic),

		// FetchMinBytes - минимальное количество байт для чтения
		// Влияет на задержку: меньше байт = быстрее ответ, но чаще запросы
		kgo.FetchMinBytes(int32(c.MinBytes)),

		// FetchMaxBytes - максимальное количество байт за один fetch
		// Защита от переполнения памяти
		kgo.FetchMaxBytes(int32(c.MaxBytes)),

		// FetchMaxWait - максимальное время ожидания данных
		// Если данных меньше MinBytes, ждём до этого таймаута
		kgo.FetchMaxWait(c.MaxWait),

		// SessionTimeout - таймаут сессии (heartbeat интервал)
		// Consumer должен отправлять heartbeat каждые SessionTimeout/3
		kgo.SessionTimeout(c.ConsumerSpecific.SessionTimeout),

		// RebalanceTimeout - таймаут ребалансировки
		// Время, которое consumer может дообрабатывать сообщения
		kgo.RebalanceTimeout(c.ConsumerSpecific.RebalanceTimeout),
	}

	// Настройка offset (с какого места читать)
	startOffset, err := c.getStartOffset()
	if err != nil {
		return nil, err // возвращаем ошибку, если offset указан неверно
	}
	// ConsumeResetOffset - опция для установки начального offset
	opts = append(opts, kgo.ConsumeResetOffset(startOffset))

	// Настройка авто-коммита
	if c.ConsumerSpecific.DisableAutoCommit || c.ConsumerSpecific.CommitInterval == 0 {
		// DisableAutoCommit - полностью отключает авто-коммит
		// Требуется явно вызывать CommitUncommittedOffsets
		opts = append(opts, kgo.DisableAutoCommit())
	} else {
		// AutoCommitInterval - авто-коммит через указанный интервал
		opts = append(opts, kgo.AutoCommitInterval(c.ConsumerSpecific.CommitInterval))
	}

	return opts, nil
}

// getStartOffset - преобразует строковый offset в формат franz-go
// Конвертирует человеко-читаемые строки ("earliest", "latest")
// в специальный тип kgo.Offset, который понимает библиотека
func (c *FranzConsumerConfig) getStartOffset() (kgo.Offset, error) {
	switch c.ConsumerSpecific.StartOffset {
	case "earliest":
		// AtStart - читать с самого начала партиции
		return kgo.NewOffset().AtStart(), nil
	case "latest":
		// AtEnd - читать только новые сообщения (начиная с текущего конца)
		return kgo.NewOffset().AtEnd(), nil
	case "at_end":
		// Синоним для latest (для совместимости)
		return kgo.NewOffset().AtEnd(), nil
	case "at_start":
		// Синоним для earliest (для совместимости)
		return kgo.NewOffset().AtStart(), nil
	default:
		// Если значение не распознано - возвращаем ошибку
		// При этом StartOffset по умолчанию AtEnd (latest)
		return kgo.NewOffset().AtEnd(), fmt.Errorf("invalid start_offset: %s (must be 'earliest' or 'latest')",
			c.ConsumerSpecific.StartOffset)
	}
}

// Validate - валидация конфигурации
// Проверяет, что все обязательные поля заполнены корректно
// Возвращает nil, если конфигурация валидна
func (c *FranzConsumerConfig) Validate() error {
	// Проверка: список брокеров не может быть пустым
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers list cannot be empty")
	}

	// Проверка: название топика обязательно
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	// Проверка: ID группы потребителей обязателен
	// Без GroupID consumer не может участвовать в consumer group
	if c.GroupID == "" {
		return fmt.Errorf("group_id cannot be empty")
	}

	// MinBytes должен быть хотя бы 1 (не может быть 0 или отрицательным)
	if c.MinBytes < 1 {
		return fmt.Errorf("min_bytes must be >= 1")
	}

	// MaxBytes должен быть хотя бы 1
	if c.MaxBytes < 1 {
		return fmt.Errorf("max_bytes must be >= 1")
	}

	// MaxWait должен быть положительным (не может быть 0 или отрицательным)
	if c.MaxWait <= 0 {
		return fmt.Errorf("max_wait must be > 0")
	}

	// Валидация специфичных полей consumer
	// Допустимые значения для start_offset
	validOffsets := map[string]bool{"earliest": true, "latest": true, "at_end": true, "at_start": true}
	if !validOffsets[c.ConsumerSpecific.StartOffset] {
		return fmt.Errorf("start_offset must be 'earliest' or 'latest', got: %s", c.ConsumerSpecific.StartOffset)
	}

	// SessionTimeout должен быть положительным
	if c.ConsumerSpecific.SessionTimeout <= 0 {
		return fmt.Errorf("session_timeout must be > 0")
	}

	// RebalanceTimeout должен быть положительным
	if c.ConsumerSpecific.RebalanceTimeout <= 0 {
		return fmt.Errorf("rebalance_timeout must be > 0")
	}

	// MaxRecordsPerFetch должен быть хотя бы 1
	if c.ConsumerSpecific.MaxRecordsPerFetch <= 0 {
		return fmt.Errorf("max_records_per_fetch must be >= 1")
	}

	// Все проверки пройдены
	return nil
}

// LoadFranzConsumerConfig - загружает конфиг консьюмера из YAML файла
func LoadFranzConsumerConfig(configPath string) (*FranzConsumerConfig, error) {
	// LoadYAMLConfig - универсальный загрузчик YAML
	return LoadYAMLConfig[FranzConsumerConfig](configPath, DefaultFranzConsumerConfig)
}
