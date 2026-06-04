package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type CacheRedisAdapter struct {
	client *redis.Client
}

// конструктор для адаптера кэша на базе Redis
func NewCacheAdapter(client *redis.Client) *CacheRedisAdapter {
	return &CacheRedisAdapter{client: client}
}

// метод для завершения работы экземпляра redis
func (r *CacheRedisAdapter) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// метод для добавления значения с TTL в redis
func (r *CacheRedisAdapter) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// метод получения значения из redis по ключу
func (r *CacheRedisAdapter) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

// метод получения значения из redis по ключу (результат в виде байтового среза)
func (r *CacheRedisAdapter) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return r.client.Get(ctx, key).Bytes()
}

// метод удаления элемента по ключу из redis
func (r *CacheRedisAdapter) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// метод проверки существования элемента в redis по ключу
func (r *CacheRedisAdapter) Exists(ctx context.Context, key string) (bool, error) {
	result, err := r.client.Exists(ctx, key).Result()
	return result > 0, err
}

// метод устанавливает время жизни ключа в Redis.
func (r *CacheRedisAdapter) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.client.Expire(ctx, key, expiration).Err()
}

// метод возвращает оставшееся время жизни ключа в Redis.
// Возвращает -1 если время жизни не установлено, -2 если ключ не существует.
func (r *CacheRedisAdapter) TTL(ctx context.Context, key string) (time.Duration, error) {
	return r.client.TTL(ctx, key).Result()
}
