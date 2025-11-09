package redisx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bytedance/sonic"
	"github.com/kanengo/ku/slicex"
	"github.com/redis/go-redis/v9"
)

var (
	ErrRedisNil = redis.Nil
)

type redisWrapperEncoder[V any] interface {
	Encode(V) (string, error)
	Decode(string) (V, error)
}

type RedisWrapper[V any] struct {
	rdb redis.Cmdable

	source func() (V, error)

	encoder redisWrapperEncoder[V]

	expiration time.Duration
}

func NewRedisWrapper[V any](rdb redis.Cmdable) RedisWrapper[V] {
	return RedisWrapper[V]{rdb: rdb}
}

func (w RedisWrapper[V]) Expiration(expiration time.Duration) RedisWrapper[V] {
	cp := w
	cp.expiration = expiration
	return cp
}

func (w RedisWrapper[V]) Encoder(encoder redisWrapperEncoder[V]) RedisWrapper[V] {
	cp := w
	cp.encoder = encoder
	return cp
}

func (w RedisWrapper[V]) Source(fn func() (V, error)) RedisWrapper[V] {
	cp := w
	cp.source = fn
	return cp
}

func (w RedisWrapper[V]) Get(ctx context.Context, key string) (V, error) {
	var zero V
	if key == "" {
		return zero, ErrRedisNil
	}

	data, err := w.rdb.Get(ctx, key).Result()
	if err == nil {
		if w.encoder != nil {
			return w.encoder.Decode(data)
		}
		var result V
		err = sonic.UnmarshalString(data, &result)
		return result, err
	}
	if err != redis.Nil {
		var zero V
		return zero, err
	}

	// cache miss
	if w.source == nil {
		var zero V
		return zero, ErrRedisNil
	}

	result, err := w.source()
	if err != nil {
		var zero V
		return zero, err
	}

	// cache hit
	if w.encoder != nil {
		data, err = w.encoder.Encode(result)
	} else {
		data, err = sonic.MarshalString(result)
	}
	if err != nil {
		var zero V
		return zero, err
	}

	err = w.rdb.Set(ctx, key, data, w.expiration).Err()
	if err != nil {
		var zero V
		return zero, err
	}

	return result, nil
}

type RedisBatchWrapper[K comparable, V any] struct {
	rdb redis.Cmdable

	keyFormat func(k K) string

	source func([]K, map[K]V) error

	encoder redisWrapperEncoder[V]

	expiration time.Duration

	usePipeline bool
}

func NewRedisBatchWrapper[K comparable, V any](rdb redis.Cmdable) RedisBatchWrapper[K, V] {
	return RedisBatchWrapper[K, V]{
		rdb:         rdb,
		usePipeline: true,
		keyFormat: func(k K) string {
			return fmt.Sprintf("%v", k)
		},
	}
}

func (w RedisBatchWrapper[K, V]) WithoutPipeline() RedisBatchWrapper[K, V] {
	cp := w
	cp.usePipeline = false

	return cp
}

func (w RedisBatchWrapper[K, V]) KeyFormat(f func(k K) string) RedisBatchWrapper[K, V] {
	cp := w
	cp.keyFormat = f

	return cp
}

func (w RedisBatchWrapper[K, V]) Encoder(enc redisWrapperEncoder[V]) RedisBatchWrapper[K, V] {
	cp := w
	cp.encoder = enc

	return cp
}

func (w RedisBatchWrapper[K, V]) Expiration(expiration time.Duration) RedisBatchWrapper[K, V] {
	cp := w
	cp.expiration = expiration

	return cp
}

func (w RedisBatchWrapper[K, V]) Source(source func([]K, map[K]V) error) RedisBatchWrapper[K, V] {
	cp := w
	cp.source = source

	return cp
}

func (w *RedisBatchWrapper[K, V]) Get(ctx context.Context, ks []K) (map[K]V, error) {
	keys := make([]string, 0, len(ks))

	for _, k := range ks {
		keys = append(keys, w.keyFormat(k))
	}

	ret := make(map[K]V, len(ks))

	if w.usePipeline {
		pl := w.rdb.Pipeline()
		for _, key := range keys {
			pl.Get(ctx, key)
		}
		cmds, err := pl.Exec(ctx)
		if err != nil {
			slog.Warn("batch get failed", "err", err, "list", ks)
		}
		for i, cmd := range cmds {
			data, err := cmd.(*redis.StringCmd).Result()
			if err == nil {
				v, err := w.decode(data)
				if err == nil {
					ret[ks[i]] = v
				}
			}
		}
	} else {
		ss, err := w.rdb.MGet(ctx, keys...).Result()
		if err != nil {
			slog.Warn("batch get failed", "err", err, "list", ks)
		} else {
			for i, v := range ss {
				if v == nil {
					continue
				}
				data, ok := v.(string)
				if ok {
					v, err := w.decode(data)
					if err == nil {
						ret[ks[i]] = v
					}
				}
			}
		}
	}

	cnt := slicex.Count(ks, func(k K) bool {
		if _, ok := ret[k]; !ok {
			return true
		}
		return false
	})

	if cnt > 0 {
		noCacheKs := make([]K, 0, cnt)
		for _, k := range ks {
			if _, ok := ret[k]; !ok {
				noCacheKs = append(noCacheKs, k)
			}
		}

		if w.source == nil {
			return ret, nil
		}

		if err := w.source(noCacheKs, ret); err != nil {
			return nil, err
		}

		// write back to redis
		pl := w.rdb.Pipeline()
		for _, k := range noCacheKs {
			v := ret[k]
			var data string
			var err error
			if w.encoder != nil {
				data, err = w.encoder.Encode(v)
			} else {
				data, err = sonic.MarshalString(v)
			}
			if err != nil {
				slog.Warn("encode failed", "err", err, "key", k, "val", v)
			} else {
				pl.Set(ctx, w.keyFormat(k), data, w.expiration)
			}
		}
		_, err := pl.Exec(ctx)
		if err != nil {
			slog.Error("write back failed", "err", err)
		}

	}

	return ret, nil
}

func (w *RedisBatchWrapper[K, V]) decode(data string) (V, error) {
	var t V
	var err error
	if w.encoder != nil {
		t, err = w.encoder.Decode(data)
		if err != nil {
			slog.Warn("decode failed", "err", err, "data", data)
			return t, err
		}
		return t, nil
	} else {
		err := sonic.UnmarshalString(data, &t)
		if err != nil {
			slog.Warn("decode failed", "err", err, "data", data)
			return t, err
		}
		return t, nil
	}
}
