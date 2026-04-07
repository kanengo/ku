package dq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
		if batchSize <= 0 {
			return
		}
		o.batchSize = batchSize
	}
}

func WithPollingInterval(pollingInterval time.Duration) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		if pollingInterval <= 0 {
			return
		}
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
		if exp <= 0 {
			return
		}
		o.expiration = exp
	}
}

func WithConcurrency(num int) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		if num <= 0 {
			return
		}
		o.concurrency = num
	}
}

func WithQueueDepth(depth int) BatchDelayQueueOption {
	return func(o *BatchDelayQOptions) {
		if depth < 0 {
			return
		}
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

func (bq *BatchDelayQueue) retryQueueByShardKey(shardKey string) string {
	return shardKey + ":retry"
}

func (bq *BatchDelayQueue) pendingQueue(shard int) string {
	return fmt.Sprintf("bdq:{%s:%d}:pending", bq.opts.queue, shard)
}

func (bq *BatchDelayQueue) pendingQueueByShardKey(shardKey string) string {
	return shardKey + ":pending"
}

func (bq *BatchDelayQueue) taskKey(shardKey string, id string) string {
	return shardKey + ":" + id
}

func (bq *BatchDelayQueue) enqueueByOneShard(ctx context.Context, tasks ...DelayTask) error {
	if len(tasks) == 0 {
		return nil
	}
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
	if len(tasks) == 0 {
		return nil
	}
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
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("LREM", KEYS[3], 0, ARGV[1])
redis.call("DEL", KEYS[4])
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

	return removeTaskScript.Run(ctx, bq.rdb, []string{
		shardKey,
		bq.pendingQueueByShardKey(shardKey),
		bq.retryQueueByShardKey(shardKey),
		taskKey,
	}, key).Err()
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
local values2 = redis.call("ZRANGE", KEYS[2], "-inf", ARGV[2], "BYSCORE", "LIMIT", 0, left)
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

var cleanupMissingDelayTasksScript = redis.NewScript(`
for i, id in ipairs(ARGV) do
	redis.call("ZREM", KEYS[1], id)
	redis.call("ZREM", KEYS[2], id)
	redis.call("LREM", KEYS[3], 0, id)
	redis.call("DEL", KEYS[i + 3])
end
return #ARGV
`)

func (bq *BatchDelayQueue) cleanupMissingTasks(ctx context.Context, shard int, ids []string) {
	if len(ids) == 0 {
		return
	}

	shardKey := bq.queue(shard)
	keys := stringslicepool.Get(len(ids) + 3)[:0]
	defer stringslicepool.Put(keys)
	keys = append(keys, shardKey, bq.pendingQueue(shard), bq.retryQueue(shard))

	args := make([]any, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, bq.taskKey(shardKey, id))
		args = append(args, id)
	}

	if err := cleanupMissingDelayTasksScript.Run(ctx, bq.rdb, keys, args...).Err(); err != nil {
		contextx.Logger(ctx).Warn("[BatchDelayQueue] cleanup missing task failed", "err", err, "queue", bq.opts.queue, "shard", shard)
	}
}

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

		for {
			if ctx.Err() != nil {
				return
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
				break
			}

			if len(values) == 0 {
				break
			}
			taskKeys := slicex.Map(values, func(id string, _ int) string {
				return bq.taskKey(shardKey, id)
			})

			taskContentList, err := bq.rdb.MGet(ctx, taskKeys...).Result()
			if err != nil {
				logger.Warn("[BatchDelayQueue] mget failed", "err", err, "queue", bq.opts.queue, "shard", shard)
				break
			}
			ts := make([]Task, 0, len(taskContentList))
			missingIDs := make([]string, 0)
			for i, content := range taskContentList {
				if content == nil {
					missingIDs = append(missingIDs, values[i])
					continue
				}

				data, ok := content.(string)
				if !ok {
					logger.Warn("[BatchDelayQueue] unexpected task payload type", "queue", bq.opts.queue, "shard", shard, "id", values[i])
					missingIDs = append(missingIDs, values[i])
					continue
				}

				ts = append(ts, Task{
					Id:   values[i],
					Data: data,
				})
			}
			bq.cleanupMissingTasks(ctx, shard, missingIDs)
			if len(ts) != 0 {
				select {
				case bq.queueCh <- wrappedTasks{
					ctx:   ctx,
					ts:    ts,
					shard: shard,
				}:
				case <-ctx.Done():
					return
				}
			}
			if int64(len(values)) < bq.opts.batchSize {
				break
			}
		}
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

var ackDelayTasksScript = redis.NewScript(`
for _, id in ipairs(ARGV) do
	redis.call("ZREM", KEYS[1], id)
end
for i = 2, #KEYS do
	redis.call("DEL", KEYS[i])
end
return #ARGV
`)

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
	doneList := stringslicepool.Get(len(ackList) + 1)[:0]
	defer stringslicepool.Put(doneList)
	doneList = append(doneList, bq.pendingQueue(msg.shard))
	args := make([]any, 0, len(ackList))
	for _, id := range ackList {
		doneList = append(doneList, bq.taskKey(shardKey, id))
		args = append(args, id)
	}

	if err := ackDelayTasksScript.Run(msg.ctx, bq.rdb, doneList, args...).Err(); err != nil {
		logger.Error("[BatchDelayQueue] ack failed", "err", err, "queue", bq.opts.queue, "shard", msg.shard)
	}
}

func (bq *BatchDelayQueue) reclaimPendingMessagesLoop(ctx context.Context, shard int) {
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
local values = redis.call("ZRANGE", KEYS[1], "-inf", ARGV[2], "BYSCORE", "LIMIT", 0, batchSize)
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
