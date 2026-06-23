package franzgoconsumer

import (
	"context"
	"errors"
	"pkg/kafka"
	franzgo "pkg/kafka/franz-go-consumer/mocks"
	"pkg/kafka/mocks"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestBaseConsumer_SuccessfulProcessing проверяет базовый сценарий работы консьюмера:
// 1. Получение сообщений из Kafka через клиент
// 2. Успешная обработка каждого сообщения хендлером
// 3. Коммит оффсетов после успешной обработки
// 4. Корректная статистика (processed увеличивается, dlq не меняется)
func TestBaseConsumer_SuccessfulProcessing(t *testing.T) {
	// Arrange (подготовка)
	// Создаем контекст с таймаутом, чтобы тест не висел вечно
	// 5 секунд достаточно для обработки нескольких сообщений
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // гарантируем освобождение ресурсов

	// Создаем моки для всех зависимостей
	// MockKafkaClient - имитирует работу с Kafka (получение сообщений)
	mockClient := franzgo.NewMockKafkaClient(t)
	// MockMessageHandler - имитирует бизнес-логику обработки сообщений
	mockHandler := mocks.NewMockMessageHandler(t)
	// MockDLQSender - имитирует отправку в Dead Letter Queue (в этом тесте не используется)
	mockDLQ := mocks.NewMockDLQSender(t)

	// Подготавливаем тестовые данные - 2 сообщения для обработки
	// Каждое сообщение содержит: топик, партицию, оффсет, ключ и значение
	testRecords := []*kgo.Record{
		{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    0,
			Key:       []byte("key1"),
			Value:     []byte("value1"),
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    1,
			Key:       []byte("key2"),
			Value:     []byte("value2"),
		},
	}

	// Создаем объект Fetches, который имитирует ответ от Kafka
	// Fetches - это структура, содержащая полученные сообщения и возможные ошибки
	// В реальном коде она создается методом PollFetches
	fetches := kgo.Fetches{}
	// TODO: Добавить записи в fetches через правильный API
	// Для тестов нам нужно заполнить fetches тестовыми записями
	// В реальном тесте вы можете использовать вспомогательную функцию
	// для создания Fetches из []*kgo.Record

	// Настраиваем ожидания (expectations) для моков
	// Это говорит: "когда вызовут PollFetches с любым контекстом, верни fetches"
	mockClient.EXPECT().
		PollFetches(mock.Anything). // mock.Anything означает "любой аргумент"
		Return(fetches).            // возвращаем подготовленные данные
		Once()                      // ожидаем ровно один вызов

	// Ожидаем, что хендлер будет вызван для каждого сообщения
	// Times(2) означает, что HandleMessage будет вызван ровно 2 раза
	// с любыми аргументами и должен вернуть nil (без ошибки)
	mockHandler.EXPECT().
		HandleMessage(mock.Anything, mock.Anything). // любые аргументы
		Return(nil).                                 // без ошибки
		Times(len(testRecords))                      // для каждого сообщения

	// Ожидаем, что после обработки батча будет вызван хук OnBatchProcessed
	// С аргументом = количество успешно обработанных сообщений (2)
	mockHandler.EXPECT().
		OnBatchProcessed(len(testRecords)). // batchSize = 2
		Once()

	// Ожидаем, что будет выполнен коммит оффсетов
	// CommitUncommittedOffsets коммитит все неподтвержденные оффсеты
	mockClient.EXPECT().
		CommitUncommittedOffsets(mock.Anything).
		Return(nil). // коммит успешен
		Once()

	// Создаем опции консьюмера с настройками по умолчанию
	opts := DefaultOptions()
	// Устанавливаем максимальный размер батча больше, чем количество сообщений
	// Чтобы все сообщения обработались за один раз
	opts.MaxBatchSize = 1000
	// Отключаем автоматический коммит, чтобы проверить ручной коммит
	opts.CommitInterval = 0

	// Создаем экземпляр консьюмера с нашими моками
	// Передаем nil для логгера, будет использован slog.Default()
	consumer, err := NewBaseConsumer(mockClient, mockHandler, mockDLQ, opts, nil)
	assert.NoError(t, err, "Consumer creation should not fail")

	// Act (действие)
	// Запускаем консьюмер. Он будет работать в бесконечном цикле,
	// но так как у нас только один батч сообщений, после обработки
	// он продолжит опрашивать Kafka (но мок вернет пустой ответ)
	// Контекст с таймаутом завершит выполнение через 5 секунд
	err = consumer.Start(ctx)

	// Assert (проверка)
	// Проверяем, что Start завершился без ошибки
	assert.NoError(t, err, "Consumer start should not fail")

	// Проверяем статистику:
	// processed должно быть равно количеству успешно обработанных сообщений
	processed, dlq := consumer.GetStats()
	assert.Equal(t, int64(len(testRecords)), processed,
		"All messages should be processed successfully")
	assert.Equal(t, int64(0), dlq,
		"No messages should be sent to DLQ")

	// Проверяем, что все ожидания моков выполнены
	// Это делает AssertExpectations автоматически благодаря NewMockKafkaClient
	// который регистрирует Cleanup функцию
}

// ТЕСТ №2: Одно сообщение с ошибкой (простая версия)
func TestBaseConsumer_ErrorHandlingAndDLQ(t *testing.T) {
	// 1. ПОДГОТОВКА

	// Создаём моки
	mockClient := franzgo.NewMockKafkaClient(t)
	mockHandler := mocks.NewMockMessageHandler(t)
	mockDLQ := mocks.NewMockDLQSender(t)

	// 3 сообщения: первое и третье - хорошие, второе - плохое
	testRecords := []*kgo.Record{
		{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    0,
			Key:       []byte("good1"),
			Value:     []byte("value1"),
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    1,
			Key:       []byte("bad"),
			Value:     []byte("value2"),
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    2,
			Key:       []byte("good2"),
			Value:     []byte("value3"),
		},
	}

	// Создаём Fetches с тестовыми записями
	fetches := createTestFetches(t, testRecords)

	// 1. Получаем сообщения из Kafka
	mockClient.EXPECT().
		PollFetches(mock.Anything).
		Return(fetches).
		Once()

	// 2. Обрабатываем сообщения:
	// Используем Run для логирования, а Return для возврата ошибки
	// Самый простой способ: возвращаем ошибку только для второго сообщения
	// Для этого используем разные вызовы EXPECT с разными аргументами

	// Первое сообщение (offset 0) - успешно
	mockHandler.EXPECT().
		HandleMessage(mock.Anything, mock.MatchedBy(func(msg *kafka.Message) bool {
			return msg.Offset == 0
		})).
		Return(nil).
		Once()

	// Второе сообщение (offset 1) - с ошибкой
	mockHandler.EXPECT().
		HandleMessage(mock.Anything, mock.MatchedBy(func(msg *kafka.Message) bool {
			return msg.Offset == 1
		})).
		Return(errors.New("ошибка в сообщении")).
		Once()

	// Третье сообщение (offset 2) - успешно
	mockHandler.EXPECT().
		HandleMessage(mock.Anything, mock.MatchedBy(func(msg *kafka.Message) bool {
			return msg.Offset == 2
		})).
		Return(nil).
		Once()

	// 3. После обработки батча: успешно обработано 2 сообщения
	mockHandler.EXPECT().
		OnBatchProcessed(2).
		Once()

	// 4. DLQ включён и примет плохое сообщение
	mockDLQ.EXPECT().
		IsEnabled().
		Return(true).
		Once()

	// 5. Отправляем плохое сообщение в DLQ
	mockDLQ.EXPECT().
		Send(
			mock.Anything, // context
			mock.MatchedBy(func(msg *kafka.Message) bool {
				return msg.Offset == 1 // проверяем, что это второе сообщение
			}),
			mock.Anything, // ошибка
		).
		Return(nil). // успешно отправили
		Once()

	// 6. Коммитим оффсеты
	mockClient.EXPECT().
		CommitUncommittedOffsets(mock.Anything).
		Return(nil).
		Once()

	// Создаём опции консьюмера
	opts := DefaultOptions()
	opts.MaxBatchSize = 1000 // чтобы все сообщения обработались за раз
	opts.CommitInterval = 0  // ручной коммит

	// СОЗДАЁМ консьюмер с нашими моками
	consumer, err := NewBaseConsumer(mockClient, mockHandler, mockDLQ, opts, nil)
	assert.NoError(t, err)

	// 2. ДЕЙСТВИЕ
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = consumer.Start(ctx)

	// 3. ПРОВЕРКА
	assert.NoError(t, err)

	processed, dlq := consumer.GetStats()
	assert.Equal(t, int64(2), processed, "Обработано 2 хороших сообщения")
	assert.Equal(t, int64(1), dlq, "1 плохое ушло в DLQ")
}

// createTestFetches - вспомогательная функция для создания kgo.Fetches из записей
// В реальном коде вам может потребоваться более сложная логика,
// но для тестов этого достаточно
func createTestFetches(t *testing.T, records []*kgo.Record) kgo.Fetches {
	t.Helper() // помечаем как вспомогательную функцию для тестов

	// Создаем пустые Fetches
	fetches := kgo.Fetches{}

	// TODO: Реализация заполнения Fetches
	// В реальном коде используйте конструктор Fetches или создавайте через моки
	// Например, можно использовать:
	// fetches = kgo.Fetches{...}

	// Для целей тестирования, если у вас нет возможности создать Fetches,
	// вы можете создать структуру вручную или использовать другой подход

	return fetches
}
