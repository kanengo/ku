package cachex

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

func WithLRUContext(ctx context.Context) func(*lruOption) {
	return func(o *lruOption) {
		o.ctx = ctx
	}
}

func WithLRUDelPubSub(cmd redisPubSubCmdInterface, delPubSubChannel string) func(*lruOption) {
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
		go l.subscribeDel(context.Background())
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
		err := l.options.delPubSubCmd.Publish(context.Background(),
			l.options.delPubSubChannel,
			fmt.Sprintf("%v", key),
		).Err()
		if err != nil {
			slog.Error("failed to publish", "err", err, slog.Any("key", key))
		}
	}
}

func (l *LRU[K, V]) subscribeDel(ctx context.Context) {
	ps := l.options.delPubSubCmd.Subscribe(ctx, l.options.delPubSubChannel)
	for {
		select {
		case msg := <-ps.Channel():
			var k K
			switch any(k).(type) {
			case string:
				k = any(msg.Payload).(K)
			case int32:
				t := convertx.StringToInteger[int32](msg.Payload)
				k = any(t).(K)
			case int64:
				t := convertx.StringToInteger[int64](msg.Payload)
				k = any(t).(K)
			case uint32:
				t := convertx.StringToInteger[uint32](msg.Payload)
				k = any(t).(K)
			case uint64:
				t := convertx.StringToInteger[uint64](msg.Payload)
				k = any(t).(K)
			case uint16:
				t := convertx.StringToInteger[uint16](msg.Payload)
				k = any(t).(K)
			case uint8:
				t := convertx.StringToInteger[uint8](msg.Payload)
				k = any(t).(K)
			case int:
				t := convertx.StringToInteger[int](msg.Payload)
				k = any(t).(K)
			case int16:
				t := convertx.StringToInteger[int16](msg.Payload)
				k = any(t).(K)
			case uint:
				t := convertx.StringToInteger[uint](msg.Payload)
				k = any(t).(K)
			default:
				slog.Warn("unexpected key type", slog.String("key", msg.Payload), slog.String("type", fmt.Sprintf("%T", k)))
				continue
			}
			l.c.Remove(k)
		case <-ctx.Done():
			ps.Close()
			return
		}
	}
}
