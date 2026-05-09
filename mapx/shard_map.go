package mapx

import (
	"cmp"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const (
	defaultShardNum = 64
)

type ShardMapKey interface {
	ShardKey() string
}

type ShardMap[TK cmp.Ordered, TV any] struct {
	ShardMapOptions[TK, TV]
	shards []shardMap[TK, TV]
	len    atomic.Int64
}

type ShardMapOptions[TK cmp.Ordered, TV any] struct {
	ShardNum int
	KeyFunc  func(TK, TV) string
}

type shardMap[TK cmp.Ordered, TV any] struct {
	mu   sync.RWMutex
	data map[TK]TV
}

func defaultShardKeyFunc[TK cmp.Ordered, TV any](k TK, v TV) string {
	return fmt.Sprintf("%v", k)
}

func normalizeShardMapOptions[TK cmp.Ordered, TV any](opts *ShardMapOptions[TK, TV]) {
	if opts.ShardNum <= 0 {
		opts.ShardNum = defaultShardNum
	}
	if opts.KeyFunc == nil {
		opts.KeyFunc = defaultShardKeyFunc
	}
}

func (m *ShardMap[TK, TV]) getShard(k TK, v TV) int {
	sharKey := m.KeyFunc(k, v)
	sum := xxhash.Sum64String(sharKey)
	return int(sum % uint64(m.ShardNum))
}

func (m *ShardMap[TK, TV]) getShardByKey(k TK) int {
	var zero TV
	return m.getShard(k, zero)
}

func NewShardMap[TK cmp.Ordered, TV any](opts ShardMapOptions[TK, TV]) *ShardMap[TK, TV] {
	normalizeShardMapOptions(&opts)

	m := &ShardMap[TK, TV]{
		ShardMapOptions: opts,
		shards:          make([]shardMap[TK, TV], opts.ShardNum),
	}
	for i := range m.shards {
		m.shards[i].data = make(map[TK]TV)
	}
	return m
}

func (m *ShardMap[TK, TV]) Set(k TK, v TV) {
	shard := &m.shards[m.getShardByKey(k)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.data[k]; !ok {
		m.len.Add(1)
	}
	shard.data[k] = v
}

func (m *ShardMap[TK, TV]) Get(k TK) (TV, bool) {
	shard := &m.shards[m.getShardByKey(k)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	v, ok := shard.data[k]
	return v, ok
}

func (m *ShardMap[TK, TV]) Has(k TK) bool {
	_, ok := m.Get(k)
	return ok
}

func (m *ShardMap[TK, TV]) Delete(k TK) {
	shard := &m.shards[m.getShardByKey(k)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.data[k]; ok {
		m.len.Add(-1)
	}
	delete(shard.data, k)
}

func (m *ShardMap[TK, TV]) Len() int {
	return int(m.len.Load())
}

func (m *ShardMap[TK, TV]) Keys() []TK {
	keys := make([]TK, 0, m.Len())
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		for k := range shard.data {
			keys = append(keys, k)
		}
		shard.mu.RUnlock()
	}
	return keys
}

func (m *ShardMap[TK, TV]) Values() []TV {
	values := make([]TV, 0, m.Len())
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		for _, v := range shard.data {
			values = append(values, v)
		}
		shard.mu.RUnlock()
	}
	return values
}

func (m *ShardMap[TK, TV]) Range(f func(TK, TV) bool) {
	for i := range m.shards {
		items := make([]struct {
			key   TK
			value TV
		}, 0)

		shard := &m.shards[i]
		shard.mu.RLock()
		for k, v := range shard.data {
			items = append(items, struct {
				key   TK
				value TV
			}{key: k, value: v})
		}
		shard.mu.RUnlock()

		for _, item := range items {
			if !f(item.key, item.value) {
				return
			}
		}
	}
}

func (m *ShardMap[TK, TV]) Clear() {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		m.len.Add(-int64(len(shard.data)))
		shard.data = make(map[TK]TV)
		shard.mu.Unlock()
	}
}
