package dq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kanengo/ku/contextx"
	"github.com/kanengo/ku/hashx/consistenthash.go"
	"github.com/kanengo/ku/poolx/stringslicepool"
	"github.com/kanengo/ku/queuex"
	"github.com/kanengo/ku/slicex"
	"github.com/redis/go-redis/v9"
)

type BatchDelayQOptions struct {
	queue           string
	batchSize       int64
	pollingInterval time.Duration
	shardNum        int
	expiration      time.Duration
	concurrency     int
}

type BatchDelayQueueOption func(*BatchDelayQOptions)

func WithQueue(queue string) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.queue = queue
	}
}

func WithPollingBatchSize(batchSize int64) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.batchSize = batchSize
	}
}

func WithPollingInterval(pollingInterval time.Duration) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.pollingInterval = pollingInterval
	}
}
func WithShardNum(shardNum int) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		if shardNum <= 0 {
			return
		}
		o.shardNum = shardNum
	}
}

func WithExpiration(exp time.Duration) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.expiration = exp
	}
}

func WithConcurrency(num int) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.concurrency = num
	}
}

var defaultBatchDelayOptions = BatchDelayQOptions{
	queue:           "default",
	batchSize:       100,
	pollingInterval: time.Second * 1,
	shardNum:        1,
	expiration:      time.Hour * 24,
	concurrency:     1,
}

type DelayTask[T queuex.Marshaler] struct {
	Key       string
	ProcessAt int64
	Data      T
}

type BatchDelayQueue[T queuex.Marshaler] struct {
	opts BatchDelayQOptions

	wg  sync.WaitGroup
	rdb redis.Cmdable

	ctx    context.Context
	cancel context.CancelFunc

	polling atomic.Bool

	cm *consistenthash.Map
}

func NewBatchDelayQueue[T queuex.Marshaler](ctx context.Context, queue string, rdb redis.Cmdable, opts ...BatchDelayQueueOption) *BatchDelayQueue[T] {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	options := defaultBatchDelayOptions
	options.queue = queue

	for _, opt := range opts {
		opt(&options)
	}

	q := &BatchDelayQueue[T]{
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
		q.cm = cm
	}

	return q
}

func (bq *BatchDelayQueue[T]) newTaskId() string {
	u7, _ := uuid.NewV7()
	return u7.String()
}

func (bq *BatchDelayQueue[T]) queue(shard int) string {
	return fmt.Sprintf("bdq:{%s:%d}", bq.opts.queue, shard)
}

func (bq *BatchDelayQueue[T]) retryQueue(shard int) string {
	return fmt.Sprintf("bdq:{%s:%d}:retry", bq.opts.queue, shard)
}

func (bq *BatchDelayQueue[T]) taskKey(shardKey string, id string) string {
	return fmt.Sprintf("%s:%s", shardKey, id)
}

func (bq *BatchDelayQueue[T]) enqueueByOneShard(ctx context.Context, tasks ...DelayTask[T]) error {
	fields := make([]any, 0, len(tasks)*2)
	zs := make([]redis.Z, 0, len(tasks))
	for _, v := range tasks {
		content, err := v.Data.Marshal()
		if err != nil {
			contextx.Logger(ctx).Warn("[BatchDelayQueue]Enqueue Marshal", "err", err, "data", v)
			return err
		}
		taskKey := bq.taskKey(bq.queue(0), v.Key)
		fields = append(fields, taskKey, content)
		// pl.Set(ctx, taskKey, content, bq.opts.expiration)
		zs = append(zs, redis.Z{
			Score:  float64(v.ProcessAt),
			Member: v.Key,
		})
	}
	pl := bq.rdb.Pipeline()
	pl.ZAdd(ctx, bq.queue(0), zs...)
	pl.MSet(ctx, fields...)
	_, err := pl.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (bq *BatchDelayQueue[T]) Enqueue(ctx context.Context, tasks ...DelayTask[T]) error {
	for i := range tasks {
		if tasks[i].Key == "" {
			tasks[i].Key = bq.newTaskId()
		}
	}
	if bq.opts.shardNum <= 1 {
		return bq.enqueueByOneShard(ctx, tasks...)
	}

	shardTasks := make(map[string][]DelayTask[T])
	for _, v := range tasks {
		shardKey := bq.cm.Get(v.Key)
		shardTasks[shardKey] = append(shardTasks[shardKey], v)
	}

	pl := bq.rdb.Pipeline()
	for shardKey, ts := range shardTasks {
		fields := make([]any, 0, len(ts)*2)
		zs := slicex.Map(ts, func(t DelayTask[T], _ int) redis.Z {
			return redis.Z{
				Score:  float64(t.ProcessAt),
				Member: t.Key,
			}
		})
		for _, v := range ts {
			content, err := v.Data.Marshal()
			if err != nil {
				contextx.Logger(ctx).Warn("[BatchDelayQueue]Enqueue Marshal", "err", err, "data", v)
				return err
			}
			taskKey := bq.taskKey(shardKey, v.Key)
			fields = append(fields, taskKey, content)
		}
		pl.ZAdd(ctx, shardKey, zs...)
		pl.MSet(ctx, fields...)
	}
	_, err := pl.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

var removeTaskScript = redis.NewScript(`
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("DEL", KEYS[2])
return 0
`)

func (bq *BatchDelayQueue[T]) RemoveTask(ctx context.Context, key string) error {
	var shardKey string
	if bq.opts.shardNum <= 1 {
		shardKey = bq.queue(0)
	} else {
		shardKey = bq.cm.Get(key)
	}

	taskKey := bq.taskKey(shardKey, key)

	return removeTaskScript.Run(ctx, bq.rdb, []string{shardKey, taskKey}, key).Err()
}

var batchDelayQueuePollScript = redis.NewScript(`
local batchSize = tonumber(ARGV[1])
local values = redis.call("LPOP", KEYS[1], batchSize)
local retryNum = 0
if values then
	retryNum = #values
end
if retryNum >= batchSize then
	return values
end
local left = batchSize - retryNum
local values2 = redis.call("ZRANGE", KEYS[2], "-inf", ARGV[2], "BYSCORE", "LIMIT", 0, left - 1)
if #values2 > 0 then
	redis.call("ZREMRANGEBYRANK",KEYS[2], 0, #values2 - 1)
end
if not values then
	return values2
end
if #values2 > 0 then
	for _, v in ipairs(values2) do
		table.insert(values, v)
	end
end
return values
`)

func (bq *BatchDelayQueue[T]) run(ctx context.Context, shard int, handler func(ctx context.Context, tasks []DelayTask[T]) []string) {
	intervalTicker := time.NewTicker(bq.opts.pollingInterval)
	defer intervalTicker.Stop()
	logger := contextx.Logger(ctx)
	shardKey := bq.queue(shard)
	defer func() {
		bq.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-intervalTicker.C:
		}
		values, err := batchDelayQueuePollScript.Run(ctx, bq.rdb, []string{bq.retryQueue(shard),
			bq.queue(shard)}, bq.opts.batchSize, time.Now().Unix()).StringSlice()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				logger.Warn("[BatchDelayQueue] poll", "err", err, "queue", bq.opts.queue, "shard", shard)
			}
			continue
		}
		taskKeys := slicex.Map(values, func(id string, _ int) string {
			return bq.taskKey(shardKey, id)
		})

		taskContentList, err := bq.rdb.MGet(ctx, taskKeys...).Result()
		if err != nil {
			logger.Warn("[BatchDelayQueue] mget failed", "err", err, "queue", bq.opts.queue, "shard", shard)
			bq.rdb.RPush(ctx, bq.retryQueue(shard), slicex.Map(values, func(item string, _ int) any {
				return item
			})...)
			continue
		}
		data := make([]DelayTask[T], 0, len(taskContentList))
		for i, content := range taskContentList {
			if content == nil {
				continue
			}
			var v T
			err := v.Unmarshal(content.(string))
			if err != nil {
				logger.Warn("[BatchDelayQueue] poll unmarshal", "err", err, "queue", bq.opts.queue, "shard", shard)
				continue
			}
			data = append(data, DelayTask[T]{
				Key:  values[i],
				Data: v,
			})
		}
		if len(data) == 0 {
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("[BatchDelayQueue] poll panic", "err", r, "queue", bq.opts.queue, "shard", shard)
				}
			}()
			retryList := handler(ctx, data)
			if len(retryList) > 0 {
				bq.rdb.RPush(ctx, bq.retryQueue(shard), slicex.Map(
					retryList, func(v string, _ int) any {
						return v
					},
				)...)
			}
			doneList := stringslicepool.Get(len(data) - len(retryList))[:0]
			for _, id := range values {
				if slices.Contains(retryList, id) {
					continue
				}
				doneList = append(doneList, bq.taskKey(shardKey, id))
			}
			if len(doneList) > 0 {
				bq.rdb.Del(ctx, doneList...)
			}
			stringslicepool.Put(doneList)
		}()
		//
	}
}

func (bq *BatchDelayQueue[T]) poll(ctx context.Context, shard int, handler func(ctx context.Context, tasks []DelayTask[T]) []string) {
	for range bq.opts.concurrency {
		go bq.run(ctx, shard, handler)
	}
}

func (bq *BatchDelayQueue[T]) Poll(ctx context.Context, handler func(ctx context.Context, tasks []DelayTask[T]) []string) {
	if !bq.polling.CompareAndSwap(false, true) {
		return
	}
	bq.ctx, bq.cancel = context.WithCancel(ctx)
	bq.wg.Add(bq.opts.shardNum * bq.opts.concurrency)
	for shard := range bq.opts.shardNum {
		bq.poll(bq.ctx, shard, handler)
	}
}

func (bq *BatchDelayQueue[T]) Shutdown() error {
	if !bq.polling.CompareAndSwap(true, false) {
		return nil
	}
	bq.cancel()
	bq.wg.Wait()

	return nil
}
