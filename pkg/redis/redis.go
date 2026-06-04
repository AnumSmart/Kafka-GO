package redis

import (
	"context"
	"fmt"
	"global_models/global_cache"
	"log"
	configs "pkg/configs"

	"github.com/go-redis/redis/v8"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisCacheRepository(ctx context.Context, cfg *configs.RedisConfig) (global_cache.Cache, error) {
	// проверяем, что конфиг редиса не nil
	if cfg == nil {
		return nil, fmt.Errorf("Error in redis config")
	}

	// создаёем экземпляр опций, на базе которых построим клиента
	redisOptions := cfg.ToRedisOptions()

	// создаём клиента redis на основе опций *redis.Options, которые получились из клиента redis
	client := redis.NewClient(redisOptions)

	// Проверяем подключение
	ctx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	log.Printf("Connected to Redis at %s (DB: %d)", redisOptions.Addr, redisOptions.DB)

	// возвращаем результат работы конструктора адаптера
	return NewCacheAdapter(client), nil
}
