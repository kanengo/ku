package bq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"sync"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/google/uuid"
	"github.com/kanengo/ku/contextx"
	"github.com/kanengo/ku/hashx/consistenthash.go"
	"github.com/kanengo/ku/poolx/stringslicepool"
	"github.com/kanengo/ku/slicex"
	"github.com/redis/go-redis/v9"
)

type BatchPollingQOptions struct {
	queue             string
	batchSize         int64
	pollingInterval   time.Duration
	shardNum          int
	expiration        time.Duration
	concurrency       int
	queueDepth        int
	redeliverInterval time.Duration
}

type Task struct {
	Id   string
	Data string
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

func WithQueueDepth(depth int) BatchPollingQueueOption {
	return func(o *BatchPollingQOptions) {
		o.queueDepth = depth
	}
}

var defaultBatchPollingOptions = BatchPollingQOptions{
	queue:             "default",
	batchSize:         100,
	pollingInterval:   time.Second * 1,
	shardNum:          1,
	expiration:        time.Hour * 24,
	concurrency:       4,
	queueDepth:        100,
	redeliverInterval: 3 * time.Second,
}

type wrappedTasks struct {
	ctx   context.Context
	shard int
	ts    []Task
}

type BatchPollingQueue struct {
	opts BatchPollingQOptions

	wg  sync.WaitGroup
	rdb redis.Cmdable

	cm *consistenthash.Map

	polling atomic.Bool

	queueCh chan wrappedTasks

	closeCh chan struct{}
}

func NewBatchPollingQueue(ctx context.Context, queue string, rdb redis.Cmdable, opts ...BatchPollingQueueOption) (*BatchPollingQueue, error) {
	options := defaultBatchPollingOptions
	options.queue = queue
	for _, opt := range opts {
		opt(&options)
	}

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	q := &BatchPollingQueue{
		opts:    options,
		rdb:     rdb,
		queueCh: make(chan wrappedTasks, options.queueDepth),
		closeCh: make(chan struct{}),
	}

	if options.shardNum > 1 {
		cm := consistenthash.New(100, nil)
		for shard := range options.shardNum {
			key := q.queue(shard)
			cm.Add(key)
		}
		q.cm = cm
	}

	return q, nil
}

func (bq *BatchPollingQueue) queue(shard int) string {
	return fmt.Sprintf("bpq:{%s:%d}", bq.opts.queue, shard)
}

func (bq *BatchPollingQueue) retryQueue(shard int) string {
	return fmt.Sprintf("bpq:{%s:%d}:retry", bq.opts.queue, shard)
}

func (bq *BatchPollingQueue) pendingQueue(shard int) string {
	return fmt.Sprintf("bpq:{%s:%d}:pending", bq.opts.queue, shard)
}

func (bq *BatchPollingQueue) newTaskId() string {
	u7, _ := uuid.NewV7()
	return u7.String()
}

func (bq *BatchPollingQueue) taskKey(shardKey string, id string) string {
	return fmt.Sprintf("%s:%s", shardKey, id)
}

func (bq *BatchPollingQueue) enqueuWithShard(ctx context.Context, shard int, data ...string) error {
	keys := make([]any, 0, len(data))
	fields := make([]any, 0, len(data)*2)
	for _, v := range data {
		taskId := bq.newTaskId()
		taskKey := bq.taskKey(bq.queue(shard), taskId)
		fields = append(fields, taskKey, v)
		// pl.Set(ctx, taskKey, content, bq.opts.expiration)
		keys = append(keys, taskId)
	}
	pl := bq.rdb.Pipeline()
	pl.RPush(ctx, bq.queue(shard), keys...)
	pl.MSet(ctx, fields...)
	_, err := pl.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (bq *BatchPollingQueue) Enqueue(ctx context.Context, data ...string) error {
	if bq.opts.shardNum <= 1 {
		return bq.enqueuWithShard(ctx, 0, data...)
	}

	shardTasks := make(map[string][]Task)
	for _, v := range data {
		t := Task{
			Id:   bq.newTaskId(),
			Data: v,
		}
		shardKey := bq.cm.Get(t.Id)
		shardTasks[shardKey] = append(shardTasks[shardKey], t)
	}

	pl := bq.rdb.Pipeline()
	for shardKey, tasks := range shardTasks {
		keys := slicex.Map(tasks, func(t Task, _ int) any {
			return t.Id
		})
		pl.RPush(ctx, shardKey, keys...)
		fields := make([]any, 0, len(tasks)*2)
		for _, t := range tasks {
			fields = append(fields, bq.taskKey(shardKey, t.Id))
			fields = append(fields, t.Data)
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
	for _, v in ipairs(values) do
		redis.call("ZADD", KEYS[3], ARGV[2], v)
	end
	return values
end
local values2 = redis.call("LPOP", KEYS[2], batchSize - retryNum)
if not values then
	if values2 then
		for _, v in ipairs(values2) do
			redis.call("ZADD", KEYS[3], ARGV[2], v)
		end
	end
	return values2
end
if values2 then
	for _, v in ipairs(values2) do
		table.insert(values, v)
	end
end
if values then
	for _, v in ipairs(values) do
		redis.call("ZADD", KEYS[3], ARGV[2], v)
	end
end
return values
`)

func (bq *BatchPollingQueue) run(ctx context.Context, shard int) {
	intervalTicker := time.NewTicker(bq.opts.pollingInterval)
	defer intervalTicker.Stop()
	logger := contextx.Logger(ctx)
	shardKey := bq.queue(shard)
	for {
		select {
		case <-ctx.Done():
			return
		case <-intervalTicker.C:
		}
		values, err := batchPollingQueuePollScript.Run(ctx, bq.rdb, []string{
			bq.retryQueue(shard),
			bq.queue(shard),
			bq.pendingQueue(shard),
		}, bq.opts.batchSize,
			time.Now().Add(bq.opts.redeliverInterval).Unix()).StringSlice()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				logger.Warn("[BatchPollingQueue] poll", "err", err, "queue", bq.opts.queue, "shard", shard)
			}
			continue
		}

		if len(values) == 0 {
			continue
		}
		taskKeys := slicex.Map(values, func(id string, _ int) string {
			return bq.taskKey(shardKey, id)
		})

		taskContentList, err := bq.rdb.MGet(ctx, taskKeys...).Result()
		if err != nil {
			logger.Warn("[BatchPollingQueue] mget failed", "err", err, "queue", bq.opts.queue, "shard", shard)
			bq.rdb.RPush(ctx, bq.retryQueue(shard), slicex.Map(values, func(item string, _ int) any {
				return item
			})...)
			continue
		}
		data := make([]Task, 0, len(taskContentList))
		for i, content := range taskContentList {
			if content == nil {
				continue
			}

			data = append(data, Task{
				Id:   values[i],
				Data: content.(string),
			})
		}
		if len(data) == 0 {
			continue
		}
		select {
		case bq.queueCh <- wrappedTasks{
			ctx:   ctx,
			ts:    data,
			shard: shard,
		}:
		case <-ctx.Done():
			return
		}
	}
}

func (bq *BatchPollingQueue) poll(ctx context.Context, shard int) {
	go func() {
		defer bq.wg.Done()
		bq.run(ctx, shard)
	}()

}

func (bq *BatchPollingQueue) worker(handler func(ctx context.Context, tasks []Task) []string) {
	for {
		select {
		case <-bq.closeCh:
		case msg := <-bq.queueCh:
			bq.processTasks(msg, handler)
		}
	}
}

func (bq *BatchPollingQueue) processTasks(msg wrappedTasks, handler func(ctx context.Context, tasks []Task) []string) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("[BatchPollingQueue] poll panic", "err", r, "queue", bq.opts.queue, "shard", msg.shard)
		}
	}()
	ackList := handler(msg.ctx, msg.ts)
	if len(ackList) == 0 {
		return
	}
	shardKey := bq.queue(msg.shard)
	doneList := stringslicepool.Get(len(ackList))[:0]
	defer stringslicepool.Put(doneList)

	for _, id := range ackList {
		doneList = append(doneList, bq.taskKey(shardKey, id))
	}

	pl := bq.rdb.Pipeline()
	pl.Del(msg.ctx, doneList...)
	pl.ZRem(msg.ctx, bq.pendingQueue(msg.shard),
		slicex.Map(ackList, func(id string, _ int) any {
			return id
		})...,
	)
	pl.Exec(msg.ctx)
}

func (bq *BatchPollingQueue) reclaimPendingMessagesLoop(ctx context.Context, shard int) {
	bq.reclaimPendingMessages(ctx, shard)
	ticker := time.NewTicker(bq.opts.redeliverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bq.reclaimPendingMessages(ctx, shard)
		}
	}
}

var reclaimPendingMessagesScript = redis.NewScript(`
local batchSize = tonumber(ARGV[1])
local values = redis.call("ZRANGE", KEYS[1], "-inf", ARGV[2], "BYSCORE", "LIMIT", 0, batchSize - 1)
if #values > 0 then
	redis.call("ZREMRANGEBYRANK",KEYS[1], 0, #values - 1)
	redis.call("LPUSH", KEYS[2], unpack(values))
end
return 0
`)

func (bq *BatchPollingQueue) reclaimPendingMessages(ctx context.Context, shard int) {
	err := reclaimPendingMessagesScript.Run(ctx, bq.rdb, []string{
		bq.pendingQueue(shard),
		bq.retryQueue(shard),
	},
		bq.opts.queueDepth,
		time.Now().Unix(),
	).Err()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			contextx.Logger(ctx).Error("reclaimPendingMessages", "err", err, "shard", shard)
		}
		return
	}
}

func (bq *BatchPollingQueue) Poll(ctx context.Context, handler func(ctx context.Context, tasks []Task) []string) {
	if !bq.polling.CompareAndSwap(false, true) {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	bq.wg.Add(1)
	go func() {
		defer bq.wg.Done()
		defer cancel()
		select {
		case <-loopCtx.Done():
		case <-bq.closeCh:
		}
	}()
	for range bq.opts.concurrency {
		bq.wg.Add(1)
		go func() {
			defer bq.wg.Done()
			bq.worker(handler)
		}()
	}

	for shard := range bq.opts.shardNum {
		bq.wg.Add(1)
		bq.poll(loopCtx, shard)
	}

	for shard := range bq.opts.shardNum {
		bq.wg.Add(1)
		go func() {
			defer bq.wg.Done()
			bq.reclaimPendingMessagesLoop(loopCtx, shard)
		}()
	}

}

func (bq *BatchPollingQueue) Shutdown() error {
	if !bq.polling.CompareAndSwap(true, false) {
		return nil
	}

	close(bq.closeCh)
	bq.wg.Wait()

	return nil
}
