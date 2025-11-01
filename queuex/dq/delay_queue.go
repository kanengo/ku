package dq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sync"
	"time"

	"github.com/kanengo/ku/contextx"
	"github.com/kanengo/ku/slicex"
	"github.com/redis/go-redis/v9"
)

type BatchDelayQOptions struct {
	queue           string
	batchSize       int64
	pollingInterval time.Duration
}

type BatchDelayQueueOption func(*BatchDelayQOptions)

func BatchDelayQueueName(queue string) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.queue = queue
	}
}

func WithDQBatchSize(batchSize int64) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.batchSize = batchSize
	}
}

func WithPollingInterval(pollingInterval time.Duration) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.pollingInterval = pollingInterval
	}
}

var defaultBatchDelayOptions = BatchDelayQOptions{
	queue:           "default",
	batchSize:       100,
	pollingInterval: time.Second * 3,
}

type DelayTask struct {
	key       string
	ProcessAt int64
}

type BatchDelayQueue struct {
	opts BatchDelayQOptions

	wg  sync.WaitGroup
	rdb redis.Cmdable

	ctx    context.Context
	cancel context.CancelFunc
}

func NewBatchDelayQueue(ctx context.Context, queue string, rdb redis.Cmdable, opts ...BatchDelayQueueOption) *BatchDelayQueue {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	options := defaultBatchDelayOptions
	options.queue = queue

	for _, opt := range opts {
		opt(&options)
	}
	return &BatchDelayQueue{
		opts:   options,
		rdb:    rdb,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (bq *BatchDelayQueue) queue() string {
	return fmt.Sprintf("bdq:{%s}", bq.opts.queue)
}

func (bq *BatchDelayQueue) retryQueue() string {
	return fmt.Sprintf("bdq:{%s}:retry", bq.opts.queue)
}

func (bq *BatchDelayQueue) Enqueue(ctx context.Context, tasks ...DelayTask) error {
	zs := slicex.Map(tasks, func(item DelayTask, _ int) redis.Z {
		return redis.Z{
			Score:  float64(item.ProcessAt),
			Member: item.key,
		}
	})

	return bq.rdb.ZAdd(ctx, bq.queue(), zs...).Err()
}

func (bq *BatchDelayQueue) RemoveTask(ctx context.Context, key string) error {
	return bq.rdb.ZRem(ctx, bq.queue(), key).Err()
}

func (bq *BatchDelayQueue) EnqueueXX(ctx context.Context, tasks ...DelayTask) error {
	zs := slicex.Map(tasks, func(item DelayTask, _ int) redis.Z {
		return redis.Z{
			Score:  float64(item.ProcessAt),
			Member: item.key,
		}
	})

	return bq.rdb.ZAddXX(ctx, bq.queue(), zs...).Err()
}

func (bq *BatchDelayQueue) EnqueueNX(ctx context.Context, tasks ...DelayTask) error {
	zs := slicex.Map(tasks, func(item DelayTask, _ int) redis.Z {
		return redis.Z{
			Score:  float64(item.ProcessAt),
			Member: item.key,
		}
	})

	return bq.rdb.ZAddNX(ctx, bq.queue(), zs...).Err()
}

var BatchDelayQueuePollScript = redis.NewScript(`
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

func (bq *BatchDelayQueue) Poll(handler func(ctx context.Context, tasks []string) error) {
	intervalTicker := time.NewTicker(bq.opts.pollingInterval)
	defer intervalTicker.Stop()
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
					contextx.Logger(bq.ctx).Error("[panic]BatchPollingQueue", "r", r)
				}
			}()
			values, err := BatchDelayQueuePollScript.Run(bq.ctx, bq.rdb, []string{bq.retryQueue(),
				bq.queue()}, bq.opts.batchSize, time.Now().Unix()).StringSlice()
			//values, err := bq.rdb.LPopCount(ctx, bq.opts.queue, int(bq.opts.batchSize)).Result()
			if err != nil {
				if !errors.Is(err, redis.Nil) {
					contextx.Logger(bq.ctx).Warn("[BatchDelayQueue] poll", "err", err, "queue", bq.opts.queue)
				}
				return
			}
			if len(values) == 0 {
				return
			}
			tasks := make([]string, 0, len(values))
			for _, value := range values {
				tasks = append(tasks, value)
			}
			if err := handler(bq.ctx, tasks); err != nil {
				contextx.Logger(bq.ctx).Warn("[BatchDelayQueue] retry", "err", err, slog.String("queue", bq.opts.queue))
				bq.rdb.RPush(bq.ctx, bq.retryQueue(), slicex.Map(values, func(item string, _ int) any {
					return item
				})...)
			}
		}()
	}
}

func (bq *BatchDelayQueue) Shutdown() error {
	bq.cancel()
	bq.wg.Wait()
	return nil
}
