package configs

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// структура конфига для Redis
type RedisConfig struct {
	Host            string        // Хост, где расположен redis
	Port            string        // Порт для подключения
	Password        string        // Пароль
	DB              int32         // 16 пронумерованных баз данных (0-15 по умолчанию), загружаем номер
	PoolSize        int32         // Максимальное количество одновременных TCP-соединений, которые клиент может открыть к Redis
	MinIdleConns    int32         // Минимальное количество соединений, которое нужно держать открытыми
	MaxRetries      int32         // Количество повторных запросов при временных сетевых сбоях
	DialTimeout     time.Duration // Максимальное время, которое клиент ждет при установке нового TCP-соединения с Redis сервером
	ReadTimeout     time.Duration // Таймаут чтения ответа от Redis
	WriteTimeout    time.Duration // Таймаут отправки команды в Redis
	IdleTimeout     time.Duration // Таймаут, по истечении которого закрывается неиспользуемое соединение
	PoolTimeout     time.Duration // Таймаут ожидания свободного соединения
	MaxConnAge      time.Duration // Соединение живет максимум заданное время в пуле соединений
	MinRetryBackOff time.Duration // Нижняя граница интервала попыток
	MaxRetryBackOff time.Duration // Верхняя граница интервала попыток
}

// NewRedisConfigFromEnv создает конфиг Redis из переменных окружения
// prefix - префикс переменных окружения (например, "REDIS_CACHE" или "REDIS_BLACKLIST")
// Возвращает ошибку, если обязательные поля не заполнены или значения некорректны
func NewRedisConfigFromEnv(prefix string) (*RedisConfig, error) {
	// Проверка: префикс не должен быть пустым
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}

	var errors []string

	// Функция-помощник для формирования имени переменной
	makeEnvName := func(suffix string) string {
		return prefix + "_" + suffix
	}

	// Получаем значение хоста
	host, err := getRequiredEnv(makeEnvName("HOST"))
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Получаем значение порта
	port, err := getRequiredEnv(makeEnvName("PORT"))
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Получаем значение пароля
	pass, err := getRequiredEnv(makeEnvName("PASSWORD"))
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Если есть ошибки в обязательных полях - возвращаем сразу
	if len(errors) > 0 {
		return nil, fmt.Errorf("missing required environment variables for prefix %q: %s", prefix, strings.Join(errors, ", "))
	}

	// Валидируем DB (Redis поддерживает 0-15 обычно, используем дефолт 0)
	dbCount, err := getEnvAsInt32WithValidation(makeEnvName("DB"), 0, 0, 15)
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Валидируем PoolSize (разумные границы 1-1000)
	poolSize, err := getEnvAsInt32WithValidation(makeEnvName("POOL_SIZE"), 100, 1, 1000)
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Валидируем MinIdleConns
	minIdleConns, err := getEnvAsInt32WithValidation(makeEnvName("MIN_IDLE_CONNS"), 20, 1, 1000)
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Дополнительная проверка: MinIdleConns <= PoolSize
	if minIdleConns > poolSize {
		errors = append(errors, fmt.Sprintf("%s_MIN_IDLE_CONNS (%d) cannot be greater than %s_POOL_SIZE (%d)", prefix, minIdleConns, prefix, poolSize))
	}

	// Загружаем таймауты с валидацией
	dialTimeout, err := getEnvAsDurationWithValidation(makeEnvName("DIAL_TIMEOUT"), 5*time.Second, 1*time.Second, 30*time.Second)
	if err != nil {
		errors = append(errors, err.Error())
	}

	readTimeout, err := getEnvAsDurationWithValidation(makeEnvName("READ_TIMEOUT"), 3*time.Second, 100*time.Millisecond, 30*time.Second)
	if err != nil {
		errors = append(errors, err.Error())
	}

	writeTimeout, err := getEnvAsDurationWithValidation(makeEnvName("WRITE_TIMEOUT"), 3*time.Second, 100*time.Millisecond, 30*time.Second)
	if err != nil {
		errors = append(errors, err.Error())
	}

	idleTimeout, err := getEnvAsDurationWithValidation(makeEnvName("IDLE_TIMEOUT"), 5*time.Minute, 1*time.Minute, 24*time.Hour)
	if err != nil {
		errors = append(errors, err.Error())
	}

	poolTimeout, err := getEnvAsDurationWithValidation(makeEnvName("POOL_TIMEOUT"), 4*time.Minute, 1*time.Minute, 7*time.Minute)
	if err != nil {
		errors = append(errors, err.Error())
	}

	maxConnAge, err := getEnvAsDurationWithValidation(makeEnvName("MAX_CON_AGE"), 30*time.Minute, 1*time.Minute, 65*time.Minute)
	if err != nil {
		errors = append(errors, err.Error())
	}

	maxRetries, err := getEnvAsInt32WithValidation(makeEnvName("MAX_RETRIES"), 2, 0, 3)
	if err != nil {
		errors = append(errors, err.Error())
	}

	minRetryBackoff, err := getEnvAsDurationWithValidation(makeEnvName("MIN_RETRY_BACKOFF_MS"), 100*time.Millisecond, 50*time.Millisecond, 550*time.Millisecond)
	if err != nil {
		errors = append(errors, err.Error())
	}

	maxRetryBackoff, err := getEnvAsDurationWithValidation(makeEnvName("MAX_RETRY_BACKOFF_MS"), 1000*time.Millisecond, 512*time.Millisecond, 2000*time.Millisecond)
	if err != nil {
		errors = append(errors, err.Error())
	}

	// Если есть ошибки валидации - возвращаем их
	if len(errors) > 0 {
		return nil, fmt.Errorf("configuration errors for prefix %q:\n%s", prefix, strings.Join(errors, "\n"))
	}

	return &RedisConfig{
		Host:            host,
		Port:            port,
		Password:        pass,
		DB:              dbCount,
		PoolSize:        poolSize,
		MinIdleConns:    minIdleConns,
		MaxRetries:      maxRetries,
		DialTimeout:     dialTimeout,
		ReadTimeout:     readTimeout,
		WriteTimeout:    writeTimeout,
		IdleTimeout:     idleTimeout,
		PoolTimeout:     poolTimeout,
		MaxConnAge:      maxConnAge,
		MinRetryBackOff: minRetryBackoff,
		MaxRetryBackOff: maxRetryBackoff,
	}, nil
}

// для создания клиента redis необходимо передать указатель на структуру опций: *redis.Options
func (r *RedisConfig) ToRedisOptions() *redis.Options {
	return &redis.Options{
		Addr:            r.Host + ":" + r.Port,
		Password:        r.Password,
		DB:              int(r.DB),
		PoolSize:        int(r.PoolSize),
		MinIdleConns:    int(r.MinIdleConns),
		IdleTimeout:     r.IdleTimeout,
		PoolTimeout:     r.PoolTimeout,
		MaxConnAge:      r.MaxConnAge,
		DialTimeout:     r.DialTimeout,
		ReadTimeout:     r.ReadTimeout,
		WriteTimeout:    r.WriteTimeout,
		MaxRetries:      int(r.MaxRetries),
		MinRetryBackoff: r.MinRetryBackOff,
		MaxRetryBackoff: r.MaxRetryBackOff,
	}
}
