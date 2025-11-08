package dq

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

type BatchDelayQOptions struct {
	queue             string
	batchSize         int64
	pollingInterval   time.Duration
	shardNum          int
	expiration        time.Duration
	concurrency       int
	queueDepth        int
	redeliverInterval time.Duration
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

func WithQueueDepth(depth int) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		o.queueDepth = depth
	}
}

var defaultBatchDelayOptions = BatchDelayQOptions{
	queue:             "default",
	batchSize:         100,
	pollingInterval:   time.Second * 1,
	shardNum:          1,
	expiration:        time.Hour * 24,
	concurrency:       1,
	queueDepth:        100,
	redeliverInterval: 3 * time.Second,
}

type DelayTask struct {
	Id        string
	ProcessAt int64
	Data      string
}

type Task struct {
	Id   string
	Data string
}

type wrappedTasks struct {
	ctx   context.Context
	shard int
	ts    []Task
}

type BatchDelayQueue struct {
	opts BatchDelayQOptions

	wg  sync.WaitGroup
	rdb redis.Cmdable

	polling atomic.Bool

	cm *consistenthash.Map

	queueCh chan wrappedTasks

	closeCh chan struct{}
}

func NewBatchDelayQueue(ctx context.Context, queue string, rdb redis.Cmdable, opts ...BatchDelayQueueOption) (*BatchDelayQueue, error) {

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	options := defaultBatchDelayOptions
	options.queue = queue

	for _, opt := range opts {
		opt(&options)
	}

	q := &BatchDelayQueue{
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

func (bq *BatchDelayQueue) newTaskId() string {
	u7, _ := uuid.NewV7()
	return u7.String()
}

func (bq *BatchDelayQueue) queue(shard int) string {
	return fmt.Sprintf("bdq:{%s:%d}", bq.opts.queue, shard)
}

func (bq *BatchDelayQueue) retryQueue(shard int) string {
	return fmt.Sprintf("bdq:{%s:%d}:retry", bq.opts.queue, shard)
}

func (bq *BatchDelayQueue) pendingQueue(shard int) string {
	return fmt.Sprintf("bdq:{%s:%d}:pending", bq.opts.queue, shard)
}

func (bq *BatchDelayQueue) taskKey(shardKey string, id string) string {
	return fmt.Sprintf("%s:%s", shardKey, id)
}

func (bq *BatchDelayQueue) enqueueByOneShard(ctx context.Context, tasks ...DelayTask) error {
	fields := make([]any, 0, len(tasks)*2)
	zs := make([]redis.Z, 0, len(tasks))
	for _, v := range tasks {
		taskKey := bq.taskKey(bq.queue(0), v.Id)
		fields = append(fields, taskKey, v.Data)
		// pl.Set(ctx, taskKey, content, bq.opts.expiration)
		zs = append(zs, redis.Z{
			Score:  float64(v.ProcessAt),
			Member: v.Id,
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

func (bq *BatchDelayQueue) Enqueue(ctx context.Context, tasks ...DelayTask) error {
	for i := range tasks {
		if tasks[i].Id == "" {
			tasks[i].Id = bq.newTaskId()
		}
	}
	if bq.opts.shardNum <= 1 {
		return bq.enqueueByOneShard(ctx, tasks...)
	}

	shardTasks := make(map[string][]DelayTask)
	for _, v := range tasks {
		shardKey := bq.cm.Get(v.Id)
		shardTasks[shardKey] = append(shardTasks[shardKey], v)
	}

	pl := bq.rdb.Pipeline()
	for shardKey, ts := range shardTasks {
		fields := make([]any, 0, len(ts)*2)
		zs := slicex.Map(ts, func(t DelayTask, _ int) redis.Z {
			return redis.Z{
				Score:  float64(t.ProcessAt),
				Member: t.Id,
			}
		})
		for _, v := range ts {
			taskKey := bq.taskKey(shardKey, v.Id)
			fields = append(fields, taskKey, v.Data)
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

func (bq *BatchDelayQueue) RemoveTask(ctx context.Context, key string) error {
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
	for _, v in ipairs(values) do
		redis.call("ZADD", KEYS[3], ARGV[3], v)
	end
	return values
end
local left = batchSize - retryNum
local values2 = redis.call("ZRANGE", KEYS[2], "-inf", ARGV[2], "BYSCORE", "LIMIT", 0, left - 1)
if #values2 > 0 then
	redis.call("ZREMRANGEBYRANK",KEYS[2], 0, #values2 - 1)
end
if not values then
	if values2 then
		for _, v in ipairs(values2) do
			redis.call("ZADD", KEYS[3], ARGV[3], v)
		end
	end
	return values2
end
if #values2 > 0 then
	for _, v in ipairs(values2) do
		table.insert(values, v)
	end
end
if values then
	for _, v in ipairs(values) do
		redis.call("ZADD", KEYS[3], ARGV[3], v)
	end
end
return values
`)

func (bq *BatchDelayQueue) run(ctx context.Context, shard int) {
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

		if ctx.Err() != nil {
			break
		}
		now := time.Now()
		values, err := batchDelayQueuePollScript.Run(ctx, bq.rdb, []string{
			bq.retryQueue(shard),
			bq.queue(shard),
			bq.pendingQueue(shard),
		}, bq.opts.batchSize, now.Unix(),
			now.Add(bq.opts.redeliverInterval).Unix()).StringSlice()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				logger.Warn("[BatchDelayQueue] poll", "err", err, "queue", bq.opts.queue, "shard", shard)
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
			logger.Warn("[BatchDelayQueue] mget failed", "err", err, "queue", bq.opts.queue, "shard", shard)
			bq.rdb.RPush(ctx, bq.retryQueue(shard), slicex.Map(values, func(item string, _ int) any {
				return item
			})...)
			continue
		}
		ts := make([]Task, 0, len(taskContentList))
		for i, content := range taskContentList {
			if content == nil {
				continue
			}

			ts = append(ts, Task{
				Id:   values[i],
				Data: content.(string),
			})
		}
		if len(ts) == 0 {
			continue
		}
		select {
		case bq.queueCh <- wrappedTasks{
			ctx:   ctx,
			ts:    ts,
			shard: shard,
		}:
		case <-ctx.Done():
			return
		}
		//
	}
}

func (bq *BatchDelayQueue) poll(ctx context.Context, shard int) {
	go func() {
		defer bq.wg.Done()
		bq.run(ctx, shard)
	}()
}

func (bq *BatchDelayQueue) worker(handler func(ctx context.Context, tasks []Task) []string) {
	for {
		select {
		case <-bq.closeCh:
			return
		case msg := <-bq.queueCh:
			bq.processTasks(msg, handler)
		}
	}
}

func (bq *BatchDelayQueue) processTasks(msg wrappedTasks, handler func(ctx context.Context, tasks []Task) []string) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("[BatchDelayQueue] poll panic", "err", r, "queue", bq.opts.queue, "shard", msg.shard)
		}
	}()
	ackList := handler(msg.ctx, msg.ts)
	if len(ackList) == 0 {
		return
	}
	shardKey := bq.queue(msg.shard)
	doneList := stringslicepool.Get(len(ackList))[:0]
	stringslicepool.Put(doneList)
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

func (bq *BatchDelayQueue) reclaimPendingMessagesLoop(ctx context.Context, shard int) {
	bq.reclaimPendingMessages(ctx, shard)
	ticker := time.NewTicker(bq.opts.redeliverInterval)
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

func (bq *BatchDelayQueue) reclaimPendingMessages(ctx context.Context, shard int) {
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

func (bq *BatchDelayQueue) Poll(ctx context.Context, handler func(ctx context.Context, tasks []Task) []string) {
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

func (bq *BatchDelayQueue) Shutdown() error {
	if !bq.polling.CompareAndSwap(true, false) {
		return nil
	}

	close(bq.closeCh)
	bq.wg.Wait()

	return nil
}
