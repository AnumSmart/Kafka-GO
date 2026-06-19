package consumer

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// MessageRecord - структура для хранения одного сообщения из Kafka
type MessageRecord struct {
	Topic     string    // Topic - название топика, из которого получено сообщение
	Partition int32     // Partition - номер партиции (0, 1, 2...)
	Offset    int64     // Offset - смещение сообщения в партиции (уникальный идентификатор)
	Key       string    // Key - ключ сообщения (используется для маршрутизации в партицию)
	Value     string    // Value - тело сообщения (полезная нагрузка)
	Timestamp time.Time // Timestamp - время получения сообщения (когда consumer его прочитал)
}

// MessageStore - потокобезопасное хранилище сообщений в памяти
// Использует слайс для хранения и RWMutex для защиты от конкурентного доступа
type MessageStore struct {
	logger   *slog.Logger
	mu       sync.RWMutex    // mu - мьютекс для синхронизации доступа к слайсу
	messages []MessageRecord // messages - слайс для хранения записей сообщений
	maxSize  int             // максимальное количество сообщений

	// Метрики для мониторинга
	stats StoreStats
}

// StoreStats - статистика использования хранилища
type StoreStats struct {
	TotalAdded   int64     // Всего добавлено сообщений
	TotalDropped int64     // Всего удалено старых сообщений
	LastCleanup  time.Time // Время последней очистки
}

// NewMessageStore - конструктор, создаёт новое хранилище сообщений
func NewMessageStore(maxSize int, logger *slog.Logger) *MessageStore {
	if logger == nil {
		logger = slog.Default()
	}

	if maxSize <= 0 {
		maxSize = 10000
	}

	store := &MessageStore{
		logger:   logger,
		messages: make([]MessageRecord, 0, maxSize),
		maxSize:  maxSize,
	}

	store.logger.Info("MessageStore created", "max_size", maxSize)

	return store
}

// Add - добавляет сообщение в хранилище
func (s *MessageStore) Add(record MessageRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Логируем добавление на DEBUG уровне (не захламляем продакшн)
	s.logger.Debug("Adding message to store",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"key", record.Key,
		"current_size", len(s.messages),
	)

	if len(s.messages) >= s.maxSize {
		// Удаляем старые сообщения (первые 10%)
		dropCount := s.maxSize / 10
		s.messages = s.messages[dropCount:]
		s.stats.TotalDropped += int64(dropCount)
		s.stats.LastCleanup = time.Now()

		// Логируем очистку на WARN уровне (важное событие)
		s.logger.Warn("Store size limit reached, dropping old messages",
			"dropped_count", dropCount,
			"max_size", s.maxSize,
			"total_dropped", s.stats.TotalDropped,
		)
	}

	// Добавляем запись в слайс
	s.messages = append(s.messages, record)
	s.stats.TotalAdded++

	// Логируем при достижении важных вех (тут каждую 1000 записей)
	if s.stats.TotalAdded%1000 == 0 {
		s.logger.Info("Store milestone reached",
			"total_added", s.stats.TotalAdded,
			"current_size", len(s.messages),
		)
	}
}

// AddFromKafka - добавляет сообщение из Kafka Record в хранилище
func (s *MessageStore) AddFromKafka(topic string, partition int32, offset int64, key, value []byte) {
	record := MessageRecord{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       string(key),   // конвертируем []byte в string
		Value:     string(value), // конвертируем []byte в string
		Timestamp: time.Now(),
	}

	s.Add(record)
}

// GetAll - возвращает копию всех сохранённых сообщений
// Потокобезопасно: блокирует чтение, но позволяет параллельное чтение
//
// Возвращает:
//   - []MessageRecord - копия слайса с сообщениями
func (s *MessageStore) GetAll() []MessageRecord {
	// Блокируем на чтение (множественный доступ)
	// Другие горутины могут читать одновременно, но не могут писать
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.logger.Debug("Getting all messages", "count", len(s.messages))

	// Создаём копию слайса, чтобы избежать race condition
	// Если вернуть оригинал, то вызывающий код может изменить его
	result := make([]MessageRecord, len(s.messages))
	copy(result, s.messages)

	return result
}

// Count - возвращает количество сообщений в хранилище
func (s *MessageStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := len(s.messages)

	// Логируем на DEBUG уровне при каждом запросе
	s.logger.Debug("Store count requested", "count", count)

	return count
}

// GetByOffset - возвращает сообщение по номеру партиции и offset
// Полезно для поиска конкретного сообщения
func (s *MessageStore) GetByOffset(partition int32, offset int64) (*MessageRecord, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.logger.Debug("Searching message by offset",
		"partition", partition,
		"offset", offset,
	)

	for i, msg := range s.messages {
		if msg.Partition == partition && msg.Offset == offset {
			s.logger.Debug("Message found",
				"partition", partition,
				"offset", offset,
				"index", i,
			)
			return &msg, i
		}
	}

	s.logger.Debug("Message not found",
		"partition", partition,
		"offset", offset,
	)

	return nil, -1
}

// GetByKey - возвращает все сообщения с определённым ключом
func (s *MessageStore) GetByKey(key string) []MessageRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.logger.Debug("Searching messages by key", "key", key)

	result := make([]MessageRecord, 0)

	for _, msg := range s.messages {
		if msg.Key == key {
			result = append(result, msg)
		}
	}

	s.logger.Debug("Messages found by key",
		"key", key,
		"count", len(result),
	)

	return result
}

// Clear - очищает хранилище (удаляет все сообщения)
func (s *MessageStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := len(s.messages)
	// Создаём новый пустой слайс, старый уйдёт на GC
	s.messages = make([]MessageRecord, 0)

	s.logger.Info("Store cleared",
		"cleared_count", count,
		"total_added", s.stats.TotalAdded,
		"total_dropped", s.stats.TotalDropped,
	)
}

// PrintAll - выводит все сообщения в консоль в форматированном виде
// Используется при graceful shutdown для отображения накопленных сообщений
func (s *MessageStore) PrintAll() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.messages) == 0 {
		s.logger.Info("No messages stored to print")
		fmt.Println("\n📭 No messages stored")
		return
	}

	s.logger.Info("Printing all stored messages",
		"count", len(s.messages),
	)

	fmt.Println("\n" + string(repeat('=', 60)))
	fmt.Printf("📦 TOTAL MESSAGES STORED: %d\n", len(s.messages))
	fmt.Println(string(repeat('=', 60)))

	for i, msg := range s.messages {
		fmt.Printf("\n[%d] Message details:\n", i+1)
		fmt.Printf("    Topic:     %s\n", msg.Topic)
		fmt.Printf("    Partition: %d\n", msg.Partition)
		fmt.Printf("    Offset:    %d\n", msg.Offset)
		fmt.Printf("    Key:       %s\n", msg.Key)
		fmt.Printf("    Value:     %s\n", msg.Value)
		fmt.Printf("    Time:      %s\n", msg.Timestamp.Format("2006-01-02 15:04:05.000"))
	}

	fmt.Println("\n" + string(repeat('=', 60)))
}

// repeat - вспомогательная функция для повторения символа
// Используется для создания разделителей в выводе
func repeat(c byte, count int) []byte {
	result := make([]byte, count)
	for i := range result {
		result[i] = c
	}
	return result
}
