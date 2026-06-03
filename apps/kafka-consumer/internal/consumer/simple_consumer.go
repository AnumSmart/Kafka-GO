package consumer

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SimpleConsumer - простой consumer для чтения сообщений из Kafka
// Реализует базовый цикл: poll → process → commit
type SimpleConsumer struct {
	// client - клиент franz-go для взаимодействия с Kafka
	client *kgo.Client

	// messageStore - хранилище для накопления сообщений
	messageStore *MessageStore

	// statsPrintInterval - интервал вывода статистики (для мониторинга)
	statsPrintInterval time.Duration

	// lastStatsTime - время последнего вывода статистики
	lastStatsTime time.Time

	// messagesProcessed - счётчик обработанных сообщений
	messagesProcessed int64
}

// NewSimpleConsumer - конструктор, создаёт новый consumer
func NewSimpleConsumer(client *kgo.Client, store *MessageStore) *SimpleConsumer {
	return &SimpleConsumer{
		client:             client,
		messageStore:       store,
		statsPrintInterval: 10 * time.Second, // каждые 10 секунд выводим статистику
		lastStatsTime:      time.Now(),
	}
}

// Start - запуск основного цикла потребления сообщений
// Работает в бесконечном цикле до отмены контекста
//
// Параметры:
//   - ctx: контекст для graceful shutdown
//
// Возвращает:
//   - error - ошибка при работе consumer
func (c *SimpleConsumer) Start(ctx context.Context) error {
	log.Println("🚀 Starting simple consumer with franz-go...")
	log.Printf("📋 Configuration: using topic, group ID will be used from client config")

	// Счётчик для отладки количества итераций
	iteration := 0

	for {
		select {
		case <-ctx.Done():
			// Получен сигнал завершения
			log.Println("🛑 Context cancelled, stopping consumer...")
			return nil
		default:
			// Продолжаем работу
		}

		iteration++
		if iteration%100 == 0 {
			// Каждые 100 итераций выводим признак жизни
			log.Printf("💓 Consumer alive, iteration %d", iteration)
		}

		// ===== ШАГ 1: ПОЛУЧЕНИЕ СООБЩЕНИЙ (POLL) =====
		// PollFetches - блокирующий вызов, ожидает сообщения или таймаут
		// Возвращает Fetches - контейнер с сообщениями и возможными ошибками
		fetches := c.client.PollFetches(ctx)

		// ===== ШАГ 2: ПРОВЕРКА НА ОШИБКИ =====
		// Errors() возвращает слайс ошибок, возникших при получении сообщений
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				// Логируем ошибку, но продолжаем работу
				// Возможные ошибки: таймаут, потеря соединения, ошибки авторизации
				log.Printf("❌ Fetch error: topic=%s, partition=%d, error=%v",
					err.Topic, err.Partition, err.Err)
			}
			// При ошибках не коммитим, ждём следующую итерацию
			continue
		}

		// ===== ШАГ 3: ОБРАБОТКА СООБЩЕНИЙ (PROCESS) =====
		// RecordIter() возвращает итератор по всем полученным сообщениям
		iter := fetches.RecordIter()

		// Счётчик сообщений в текущем батче
		batchCount := 0

		// Итерируем по всем сообщениям в батче
		for !iter.Done() {
			record := iter.Next()
			batchCount++

			// Сохраняем сообщение в хранилище
			// key и value конвертируются из []byte в string
			c.messageStore.AddFromKafka(
				record.Topic,
				record.Partition,
				record.Offset,
				record.Key,
				record.Value,
			)

			// Увеличиваем глобальный счётчик
			c.messagesProcessed++

			// Логируем полученное сообщение (для отладки)
			log.Printf("📨 Received message: topic=%s, partition=%d, offset=%d, key=%s, value=%s",
				record.Topic, record.Partition, record.Offset,
				truncateString(string(record.Key), 50),
				truncateString(string(record.Value), 100))

			log.Printf("📊 Total stored: %d messages", c.messageStore.Count())
		}

		// Выводим статистику по батчу
		if batchCount > 0 {
			log.Printf("✅ Processed batch: %d messages", batchCount)
		}

		// Выводим периодическую статистику
		c.printStatsIfNeeded()

		// ===== ШАГ 4: КОММИТ OFFSET'ОВ (COMMIT) =====
		// CommitUncommittedOffsets - сохраняет позицию чтения в Kafka
		// Важно: коммитим ТОЛЬКО после успешной обработки
		// Если упасть до коммита, сообщения будут прочитаны снова
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			// Ошибка коммита не критична, но требует внимания
			// Сообщения могут быть обработаны повторно при рестарте
			log.Printf("⚠️ Failed to commit offsets: %v", err)
		} else if batchCount > 0 {
			log.Printf("💾 Committed offsets for %d messages", batchCount)
		}
	}
}

// truncateString - обрезает строку до указанной длины
// Используется для безопасного логирования длинных сообщений
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// printStatsIfNeeded - выводит статистику работы consumer с заданным интервалом
func (c *SimpleConsumer) printStatsIfNeeded() {
	if time.Since(c.lastStatsTime) < c.statsPrintInterval {
		return
	}

	log.Printf("📈 STATS: total messages processed: %d, stored: %d",
		c.messagesProcessed, c.messageStore.Count())

	c.lastStatsTime = time.Now()
}

// Shutdown - корректное завершение работы consumer
// Закрывает клиент и выводит все накопленные сообщения
func (c *SimpleConsumer) Shutdown() {
	log.Println("🛑 Shutting down consumer...")

	// Закрываем клиент Kafka (освобождает соединения)
	c.client.Close()
	log.Println("✅ Kafka client closed")

	// Выводим все накопленные сообщения в консоль
	c.messageStore.PrintAll()

	log.Printf("📊 Final statistics: total processed=%d, stored=%d",
		c.messagesProcessed, c.messageStore.Count())
}

// GetMessageStore - возвращает хранилище сообщений
// Полезно для внешнего доступа к накопленным данным
func (c *SimpleConsumer) GetMessageStore() *MessageStore {
	return c.messageStore
}

// GetStats - возвращает статистику работы consumer
func (c *SimpleConsumer) GetStats() (processed int64, stored int) {
	return c.messagesProcessed, c.messageStore.Count()
}
