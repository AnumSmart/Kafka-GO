package idempotency

import (
	"context"
	"fmt"
	"global_models/global_cache"
	"time"
)

type IdempotencyCache struct {
	Cache  global_cache.Cache
	prefix string
	ttl    time.Duration
}

func NewIdempotencyCache(cache global_cache.Cache, prefix string, ttlSeconds int) (*IdempotencyCache, error) {
	if cache == nil {
		return nil, fmt.Errorf("cache cannot be nil")
	}
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}
	if ttlSeconds <= 0 {
		return nil, fmt.Errorf("ttl must be positive, got %d", ttlSeconds)
	}

	return &IdempotencyCache{
		Cache:  cache,
		prefix: prefix,
		ttl:    time.Duration(ttlSeconds) * time.Second,
	}, nil
}

// IsProcessed - проверяет, было ли событие уже обработано
func (r *IdempotencyCache) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	fullKey := r.prefix + ":" + eventID
	exists, err := r.Cache.Exists(ctx, fullKey)
	if err != nil {
		return false, fmt.Errorf("failed to check event %s: %w", eventID, err)
	}
	return exists, nil
}

// MarkProcessed - отмечает событие как обработанное
func (r *IdempotencyCache) MarkProcessed(ctx context.Context, eventID string) error {
	fullKey := r.prefix + ":" + eventID
	if err := r.Cache.Set(ctx, fullKey, []byte("1"), r.ttl); err != nil {
		return fmt.Errorf("failed to mark event %s as processed: %w", eventID, err)
	}
	return nil
}

// Ping - проверка доступности кэша
func (r *IdempotencyCache) Ping(ctx context.Context) error {
	key := r.prefix + ":health"
	if err := r.Cache.Set(ctx, key, []byte("ok"), 1*time.Second); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	return r.Cache.Delete(ctx, key)
}
