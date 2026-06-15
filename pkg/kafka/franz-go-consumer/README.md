# franz-go-consumer

Consumer для Apache Kafka на franz-go с асинхронной DLQ, graceful shutdown и удобным тестированием.
Реализует общие интерфейсы pkg/kafka (Consumer, DLQSender, MessageHandler).

## 📁 Структура пакета

```
franz-go-consumer/
├── consumer.go      # BaseConsumer — основной цикл, graceful shutdown
├── dlq.go           # Асинхронный DLQ менеджер (очередь + воркеры)
├── config.go        # DLQManagerConfig — настройки DLQ
├── options.go       # ConsumerOptions — интервал статистики, debug
├── converter.go     # Конвертация kgo.Record ↔ kafka.Message
├── adapter.go       # Адаптер kgo.Client → KafkaClient (для тестов)
├── errors.go        # Ошибки пакета
├── types.go         # Интерфейс KafkaClient (для DI)
└── README.md
```

# Как работает DLQ

```
Ошибка → буфер (неблокирующий) → воркеры → бэтчирование → Kafka
                                    ↓
                              fallback-лог (при неудаче)
```

# Формат сообщения в DLQ

```
{
  "metadata": {
    "original_topic": "orders",
    "original_offset": 12345,
    "error": "json parse error",
    "service": "my-service"
  },
  "payload": {
    "key": "order_123",
    "value": "{...}",
    "headers": {...}
  }
}
```

# Graceful shutdown

Consumer корректно завершает работу:
Получает сигнал остановки
Дожидается обработки текущего батча
Не коммитит частично обработанные сообщения
Закрывает DLQ (дожидается отправки очереди)
Закрывает Kafka клиент

```
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer cancel()

consumer.Start(ctx) // при сигнале завершится gracefully
```

# 📦 Требования

Go 1.21+

github.com/twmb/franz-go

pkg/kafka (общие интерфейсы)
