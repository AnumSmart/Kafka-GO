package consumer

import (
	"fmt"
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
	mu       sync.RWMutex    // mu - мьютекс для синхронизации доступа к слайсу
	messages []MessageRecord // messages - слайс для хранения записей сообщений
}

// NewMessageStore - конструктор, создаёт новое хранилище сообщений
func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make([]MessageRecord, 0),
	}
}

// Add - добавляет сообщение в хранилище
func (s *MessageStore) Add(record MessageRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Добавляем запись в слайс
	s.messages = append(s.messages, record)
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

	return len(s.messages)
}

// GetByOffset - возвращает сообщение по номеру партиции и offset
// Полезно для поиска конкретного сообщения
func (s *MessageStore) GetByOffset(partition int32, offset int64) (*MessageRecord, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i, msg := range s.messages {
		if msg.Partition == partition && msg.Offset == offset {
			return &msg, i
		}
	}
	return nil, -1
}

// GetByKey - возвращает все сообщения с определённым ключом
func (s *MessageStore) GetByKey(key string) []MessageRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]MessageRecord, 0)

	for _, msg := range s.messages {
		if msg.Key == key {
			result = append(result, msg)
		}
	}

	return result
}

// Clear - очищает хранилище (удаляет все сообщения)
func (s *MessageStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Создаём новый пустой слайс, старый уйдёт на GC
	s.messages = make([]MessageRecord, 0)
}

// PrintAll - выводит все сообщения в консоль в форматированном виде
// Используется при graceful shutdown для отображения накопленных сообщений
func (s *MessageStore) PrintAll() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.messages) == 0 {
		fmt.Println("\n📭 No messages stored")
		return
	}

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
