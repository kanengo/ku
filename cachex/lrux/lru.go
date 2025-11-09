package lrux

import (
	"context"
	"fmt"
	"log/slog"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/kanengo/ku/convertx"
	"github.com/redis/go-redis/v9"
)

const (
	defaultLruSize = 1024 * 10
)

type lruOption struct {
	size int

	delPubSubChannel string
	delPubSubCmd     redisPubSubCmdInterface
	ctx              context.Context
}

type redisPubSubCmdInterface interface {
	redis.PubSubCmdable
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

type LRU[K comparable, V any] struct {
	options *lruOption
	c       *lru.Cache[K, V]
}

func WithContext(ctx context.Context) func(*lruOption) {
	return func(o *lruOption) {
		o.ctx = ctx
	}
}

func WithDelPubSub(cmd redisPubSubCmdInterface, delPubSubChannel string) func(*lruOption) {
	return func(o *lruOption) {
		o.delPubSubChannel = delPubSubChannel
		o.delPubSubCmd = cmd
	}
}

func NewLRU[K comparable, V any](size int, opts ...func(*lruOption)) *LRU[K, V] {
	options := &lruOption{
		size: defaultLruSize,
		ctx:  context.Background(),
	}

	for _, opt := range opts {
		opt(options)
	}

	if size > 0 {
		options.size = size
	}

	c, _ := lru.New[K, V](options.size)

	l := &LRU[K, V]{options: options, c: c}

	if l.options.delPubSubCmd != nil && l.options.delPubSubChannel != "" {
		go l.subscribeDel(options.ctx)
	}

	return l
}

func (l *LRU[K, V]) Get(key K) (V, bool) {
	return l.c.Get(key)
}

func (l *LRU[K, V]) Set(key K, value V) {
	l.c.Add(key, value)
}

func (l *LRU[K, V]) Del(key K) {
	l.c.Remove(key)
	if l.options.delPubSubCmd != nil && l.options.delPubSubChannel != "" {
		payload := l.key2String(key)
		if payload == "" {
			// 避免发布空字符串导致订阅侧误删零值键
			slog.Warn("skip publish: empty payload from key2String", slog.Any("key", key))
			return
		}
		err := l.options.delPubSubCmd.Publish(l.options.ctx,
			l.options.delPubSubChannel,
			payload,
		).Err()
		if err != nil {
			slog.Error("failed to publish", "err", err, slog.Any("key", key))
		}
	}
}

func (l *LRU[K, V]) string2Key(s string) K {
	var k K
	switch any(k).(type) {
	case string:
		k = any(s).(K)
	case int32:
		t := convertx.String2Integer[int32](s)
		k = any(t).(K)
	case int64:
		t := convertx.String2Integer[int64](s)
		k = any(t).(K)
	case int:
		t := convertx.String2Integer[int](s)
		k = any(t).(K)
	case int16:
		t := convertx.String2Integer[int16](s)
		k = any(t).(K)
	case uint32:
		t := convertx.String2UInteger[uint32](s)
		k = any(t).(K)
	case uint64:
		t := convertx.String2UInteger[uint64](s)
		k = any(t).(K)
	case uint16:
		t := convertx.String2UInteger[uint16](s)
		k = any(t).(K)
	case uint8:
		t := convertx.String2UInteger[uint8](s)
		k = any(t).(K)
	case uint:
		t := convertx.String2UInteger[uint](s)
		k = any(t).(K)
	default:
		slog.Warn("unexpected key type", slog.String("key", s), slog.String("type", fmt.Sprintf("%T", k)))
	}

	return k
}

func (l *LRU[K, V]) key2String(k K) string {
	switch kt := any(k).(type) {
	case string:
		return kt
	case int32:
		return convertx.Integer2String(kt)
	case int64:
		return convertx.Integer2String(kt)
	case uint32:
		return convertx.UInteger2String(kt)
	case uint64:
		return convertx.UInteger2String(kt)
	case uint16:
		return convertx.UInteger2String(kt)
	case uint8:
		return convertx.UInteger2String(kt)
	case int:
		return convertx.Integer2String(kt)
	case int16:
		return convertx.Integer2String(kt)
	case uint:
		return convertx.UInteger2String(kt)
	default:
		slog.Warn("unexpected key type", slog.String("type", fmt.Sprintf("%T", k)))
	}

	return ""
}

func (l *LRU[K, V]) subscribeDel(ctx context.Context) {
	ps := l.options.delPubSubCmd.Subscribe(ctx, l.options.delPubSubChannel)
	for {
		select {
		case msg := <-ps.Channel():
			k := l.string2Key(msg.Payload)
			l.c.Remove(k)
		case <-ctx.Done():
			ps.Close()
			return
		}
	}
}
