package bq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/kanengo/ku/contextx"
	"github.com/kanengo/ku/hashx/consistenthash.go"
	"github.com/kanengo/ku/poolx/stringslicepool"
	"github.com/kanengo/ku/queuex"
	"github.com/kanengo/ku/slicex"
	"github.com/redis/go-redis/v9"
)

type BatchPollingQOptions struct {
	queue           string
	batchSize       int64
	pollingInterval time.Duration
	shardNum        int
	expiration      time.Duration
	concurrency     int
}

type task struct {
	id   string
	data string
}

type Task[T any] struct {
	Id   string
	Data T
}

type BatchPollingQueueOption func(*BatchPollingQOptions)

func WithQueue(queue string) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		o.queue = queue
	}
}

func WithPollingBatchSize(batchSize int64) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		o.batchSize = batchSize
	}
}

func WithPollingInterval(pollingInterval time.Duration) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		o.pollingInterval = pollingInterval
	}
}

func WithShardNum(shardNum int) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		if shardNum <= 0 {
			return
		}
		o.shardNum = shardNum
	}
}

func WithExpiration(exp time.Duration) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		o.expiration = exp
	}
}

func WithConcurrency(num int) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		o.concurrency = num
	}
}

var defaultBatchPollingOptions = BatchPollingQOptions{
	queue:           "default",
	batchSize:       100,
	pollingInterval: time.Second * 1,
	shardNum:        1,
	expiration:      time.Hour * 24,
	concurrency:     1,
}

type BatchPollingQueue[T queuex.Marshaler] struct {
	opts BatchPollingQOptions

	wg  sync.WaitGroup
	rdb redis.Cmdable

	m consistenthash.Map

	ctx    context.Context
	cancel context.CancelFunc
}

func NewBatchPollingQueue[T queuex.Marshaler](ctx context.Context, rdb redis.Cmdable, opts ...BatchPollingQueueOption) *BatchPollingQueue[T] {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	options := defaultBatchPollingOptions

	for _, opt := range opts {
		opt(&options)
	}

	q := &BatchPollingQueue[T]{
		opts:   options,
		rdb:    rdb,
		ctx:    ctx,
		cancel: cancel,
	}

	if options.shardNum > 1 {
		cm := consistenthash.New(100, nil)
		for shard := range options.shardNum {
			key := q.queue(shard)
			cm.Add(key)
		}
	}

	return q
}

func (bq *BatchPollingQueue[T]) queue(shard int) string {
	return fmt.Sprintf("bpq:{%s:%d}", bq.opts.queue, shard)
}

func (bq *BatchPollingQueue[T]) retryQueue(shard int) string {
	return fmt.Sprintf("bpq:{%s:%d}:retry", bq.opts.queue, shard)
}

func (bq *BatchPollingQueue[T]) newTaskId() string {
	u7, _ := uuid.NewV7()
	return u7.String()
}

func (bq *BatchPollingQueue[T]) taskKey(shardKey string, id string) string {
	return fmt.Sprintf("%s:%s", shardKey, id)
}

func (bq *BatchPollingQueue[T]) enqueuWithShard(ctx context.Context, shard int, data ...T) error {
	keys := make([]any, 0, len(data))
	pl := bq.rdb.Pipeline()
	for _, v := range data {
		content, err := v.Marshal()
		if err != nil {
			contextx.Logger(ctx).Warn("[BatchPollingQueue]Enqueue Marshal", "err", err, "data", v)
			return err
		}
		taskKey := bq.taskKey(bq.queue(shard), bq.newTaskId())
		pl.Set(ctx, taskKey, content, bq.opts.expiration)
		keys = append(keys, taskKey)
	}
	pl.RPush(ctx, bq.queue(shard), keys...)
	_, err := pl.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (bq *BatchPollingQueue[T]) Enqueue(ctx context.Context, data ...T) error {
	if bq.opts.shardNum <= 1 {
		return bq.enqueuWithShard(ctx, 0, data...)
	}

	tasks := make([]task, 0, len(data))
	for _, v := range data {
		content, err := v.Marshal()
		if err != nil {
			contextx.Logger(ctx).Warn("[BatchPollingQueue]Enqueue Marshal", "err", err, "data", v)
			return err
		}
		tasks = append(tasks, task{
			id:   bq.newTaskId(),
			data: content,
		})
	}

	shardTasks := make(map[string][]task)
	for _, t := range tasks {
		shardKey := bq.m.Get(t.id)
		shardTasks[shardKey] = append(shardTasks[shardKey], t)
	}

	pl := bq.rdb.Pipeline()
	for shardKey, tasks := range shardTasks {
		keys := slicex.Map(tasks, func(t task, _ int) any {
			return t.id
		})
		pl.RPush(ctx, shardKey, keys...)
		fields := make([]any, 0, len(tasks)*2)
		for _, t := range tasks {
			fields = append(fields, bq.taskKey(shardKey, t.id))
			fields = append(fields, t.data)
		}
		pl.MSet(ctx, fields...)
	}
	_, err := pl.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

var batchPollingQueuePollScript = redis.NewScript(`
local batchSize = tonumber(ARGV[1])
local values = redis.call("LPOP", KEYS[1], batchSize)
local retryNum = 0
if values then
	retryNum = #values
end
if retryNum >= batchSize then
	return values
end
local values2 = redis.call("LPOP", KEYS[2], batchSize - retryNum)
if not values then
	return values2
end
if values2 then
	for _, v in ipairs(values2) do
		table.insert(values, v)
	end
end
return values
`)

func (bq *BatchPollingQueue[T]) poll(shard int, handler func(ctx context.Context, tasks []Task[T]) []string) {
	intervalTicker := time.NewTicker(bq.opts.pollingInterval)
	defer intervalTicker.Stop()
	logger := contextx.Logger(bq.ctx)
	shardKey := bq.queue(shard)
	bq.wg.Add(1)
	defer func() {
		bq.wg.Done()
	}()

	for {
		select {
		case <-bq.ctx.Done():
			return
		case <-intervalTicker.C:
		}
		values, err := batchPollingQueuePollScript.Run(bq.ctx, bq.rdb, []string{bq.retryQueue(shard),
			bq.queue(shard)}, bq.opts.batchSize).StringSlice()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				logger.Warn("[BatchPollingQueue] poll", "err", err, "queue", bq.opts.queue, "shard", shard)
			}
			continue
		}
		taskKeys := slicex.Map(values, func(id string, _ int) string {
			return bq.taskKey(shardKey, id)
		})

		taskContentList, err := bq.rdb.MGet(bq.ctx, taskKeys...).Result()
		if err != nil {
			logger.Warn("[BatchPollingQueue] mget failed", "err", err, "queue", bq.opts.queue, "shard", shard)
			bq.rdb.RPush(bq.ctx, bq.retryQueue(shard), slicex.Map(values, func(item string, _ int) any {
				return item
			})...)
		}
		data := make([]Task[T], 0, len(taskContentList))
		for i, content := range taskContentList {
			if content == nil {
				continue
			}
			var v T
			err := v.Unmarshal(content.(string))
			if err != nil {
				logger.Warn("[BatchPollingQueue] poll unmarshal", "err", err, "queue", bq.opts.queue, "shard", shard)
				continue
			}
			data = append(data, Task[T]{
				Id:   values[i],
				Data: v,
			})
		}
		if len(data) == 0 {
			continue
		}
		retryList := handler(bq.ctx, data)
		if len(retryList) > 0 {
			bq.rdb.RPush(bq.ctx, bq.retryQueue(shard), slicex.Map(
				retryList, func(v string, _ int) any {
					return v
				},
			))
		}
		doneList := stringslicepool.Get(len(data) - len(retryList))[:0]
		for _, id := range values {
			if slices.Contains(retryList, id) {
				continue
			}
			doneList = append(doneList, id)
		}
		if len(doneList) > 0 {
			bq.rdb.Del(bq.ctx, doneList...)
		}
		stringslicepool.Put(doneList)
		//
	}
}

func (bq *BatchPollingQueue[T]) Poll(handler func(ctx context.Context, data []T) error) {
	intervalTicker := time.NewTicker(bq.opts.pollingInterval)
	defer intervalTicker.Stop()
	logger := contextx.Logger(bq.ctx)
	for {
		select {
		case <-bq.ctx.Done():
			return
		case <-intervalTicker.C:
		}
		bq.wg.Add(1)
		func() {
			defer bq.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					logger.Error("[panic]BatchPollingQueue", "r", r)
				}
			}()
			// Iterate across all shards to poll batchSize items per shard.
			shardNum := bq.opts.shardNum
			if shardNum <= 0 {
				shardNum = 1
			}
			for shard := 0; shard < shardNum; shard++ {
				values, err := batchPollingQueuePollScript.Run(bq.ctx, bq.rdb, []string{bq.retryQueue(shard),
					bq.queue(shard)}, bq.opts.batchSize).StringSlice()
				if err != nil {
					if !errors.Is(err, redis.Nil) {
						logger.Warn("[BatchPollingQueue] poll", "err", err, "queue", bq.opts.queue, "shard", shard)
					}
					continue
				}
				data := make([]T, 0, len(values))
				for _, value := range values {
					var v T
					err := sonic.UnmarshalString(value, &v)
					if err != nil {
						logger.Warn("[BatchPollingQueue] poll unmarshal", "err", err, "queue", bq.opts.queue, "shard", shard)
						data = nil
						break
					}
					data = append(data, v)
				}
				if len(data) == 0 {
					continue
				}
				if err := handler(bq.ctx, data); err != nil {
					logger.Warn("[BatchPollingQueue] retry", "err", err, slog.String("queue", bq.opts.queue), slog.Int("shard", shard))
					bq.rdb.RPush(bq.ctx, bq.retryQueue(shard), slicex.Map(values, func(item string, _ int) any {
						return item
					})...)
				}
			}
		}()
	}
}

func (bq *BatchPollingQueue[T]) Shutdown() error {
	bq.cancel()
	bq.wg.Wait()
	return nil
}
