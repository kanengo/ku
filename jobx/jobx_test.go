package jobx

import (
	"context"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptCall struct {
	script string
	keys   []string
	args   []interface{}
}

type fakeRedisError string

func (e fakeRedisError) Error() string {
	return string(e)
}

func (fakeRedisError) RedisError() {}

type fakePipeline struct {
	redis.Pipeliner
	calls   []scriptCall
	execErr error
}

func (f *fakePipeline) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(fakeRedisError("NOSCRIPT fake"))
	return cmd
}

func (f *fakePipeline) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.calls = append(f.calls, scriptCall{
		script: script,
		keys:   append([]string(nil), keys...),
		args:   append([]interface{}(nil), args...),
	})
	return redis.NewCmd(ctx)
}

func (f *fakePipeline) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return nil, f.execErr
}

type fakeCmdable struct {
	redis.Cmdable
	pipeline   *fakePipeline
	scriptRuns []scriptCall
	pollIDs    []interface{}
	mgetValues []interface{}
	mgetErr    error
	evalHook   func(context.Context, string, []string, ...interface{}) error
}

func (f *fakeCmdable) Pipeline() redis.Pipeliner {
	return f.pipeline
}

func (f *fakeCmdable) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(fakeRedisError("NOSCRIPT fake"))
	return cmd
}

func (f *fakeCmdable) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.scriptRuns = append(f.scriptRuns, scriptCall{
		script: script,
		keys:   append([]string(nil), keys...),
		args:   append([]interface{}(nil), args...),
	})
	cmd := redis.NewCmd(ctx)

	if f.evalHook != nil {
		if err := f.evalHook(ctx, script, keys, args...); err != nil {
			cmd.SetErr(err)
			return cmd
		}
	}

	switch script {
	case pollJobsScriptSrc:
		cmd.SetVal(f.pollIDs)
	case removeIDsScriptSource, reclaimJobsScriptSource, requeueJobsScriptSource:
		cmd.SetVal(int64(len(args)))
	default:
		cmd.SetVal(int64(1))
	}

	return cmd
}

func (f *fakeCmdable) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	cmd := redis.NewSliceCmd(ctx, "mget")
	if f.mgetErr != nil {
		cmd.SetErr(f.mgetErr)
		return cmd
	}
	cmd.SetVal(append([]interface{}(nil), f.mgetValues...))
	return cmd
}

const (
	pollJobsScriptSrc = `
local batchSize = tonumber(ARGV[1])
local now = ARGV[2]
local leaseUntil = ARGV[3]
local values = redis.call("ZRANGE", KEYS[1], "-inf", now, "BYSCORE", "LIMIT", 0, batchSize)
if #values == 0 then
	return values
end
redis.call("ZREM", KEYS[1], unpack(values))
for _, id in ipairs(values) do
	redis.call("ZADD", KEYS[2], leaseUntil, id)
end
return values
`
)

func newTestQueue() *DelayQueue {
	return &DelayQueue{
		rdb: noopCmdable{},
		opts: options{
			queue:        "test_queue",
			schema:       "infra",
			table:        "jobx_delay_jobs",
			hotWindow:    time.Hour,
			retention:    2 * time.Hour,
			batchSize:    4,
			shardNum:     8,
			queueDepth:   2,
			leaseTimeout: 30 * time.Second,
		},
		queueCh:     make(chan wrappedJobs, 2),
		idGenerator: func() string { return "generated-id" },
	}
}

type noopCmdable struct {
	redis.Cmdable
}

func TestValidateOptions(t *testing.T) {
	valid := defaultOptions

	require.NoError(t, validateOptions(valid))

	cases := []struct {
		name string
		mut  func(*options)
		msg  string
	}{
		{
			name: "invalid schema",
			mut:  func(o *options) { o.schema = "bad-schema" },
			msg:  `invalid schema "bad-schema"`,
		},
		{
			name: "invalid table",
			mut:  func(o *options) { o.table = "bad.table" },
			msg:  `invalid table "bad.table"`,
		},
		{
			name: "invalid snowflake table",
			mut:  func(o *options) { o.snowflakeTable = "bad table" },
			msg:  `invalid snowflake table "bad table"`,
		},
		{
			name: "non-positive duration",
			mut:  func(o *options) { o.hotWindow = 0 },
			msg:  "duration options must be positive",
		},
		{
			name: "numeric out of range",
			mut:  func(o *options) { o.batchSize = 0 },
			msg:  "numeric options out of range",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.mut(&opts)
			err := validateOptions(opts)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOption)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}
}

func TestOptionHelpers(t *testing.T) {
	opts := defaultOptions

	WithQueue("queue-a")(&opts)
	WithSchema("schema_a")(&opts)
	WithTable("table_a")(&opts)
	WithHotWindow(2 * time.Hour)(&opts)
	WithRetention(3 * time.Hour)(&opts)
	WithChunkInterval(4 * time.Hour)(&opts)
	WithPromoteInterval(5 * time.Second)(&opts)
	WithPromoteRefreshInterval(6 * time.Second)(&opts)
	WithPollingInterval(7 * time.Second)(&opts)
	WithLeaseTimeout(8 * time.Second)(&opts)
	WithBatchSize(9)(&opts)
	WithShardNum(10)(&opts)
	WithConcurrency(11)(&opts)
	WithQueueDepth(0)(&opts)
	WithSnowflakeDSN("postgres://snowflake")(&opts)
	WithSnowflakeTable("snowflake_workers")(&opts)
	WithSnowflakeEpoch(123)(&opts)
	idGen := func() string { return "id-1" }
	WithIDGenerator(idGen)(&opts)

	assert.Equal(t, "queue-a", opts.queue)
	assert.Equal(t, "schema_a", opts.schema)
	assert.Equal(t, "table_a", opts.table)
	assert.Equal(t, 2*time.Hour, opts.hotWindow)
	assert.Equal(t, 3*time.Hour, opts.retention)
	assert.Equal(t, 4*time.Hour, opts.chunkInterval)
	assert.Equal(t, 5*time.Second, opts.promoteInterval)
	assert.Equal(t, 6*time.Second, opts.promoteRefreshInterval)
	assert.Equal(t, 7*time.Second, opts.pollingInterval)
	assert.Equal(t, 8*time.Second, opts.leaseTimeout)
	assert.Equal(t, 9, opts.batchSize)
	assert.Equal(t, 10, opts.shardNum)
	assert.Equal(t, 11, opts.concurrency)
	assert.Equal(t, 0, opts.queueDepth)
	assert.Equal(t, "postgres://snowflake", opts.snowflakeDSN)
	assert.Equal(t, "snowflake_workers", opts.snowflakeTable)
	assert.Equal(t, int64(123), opts.snowflakeEpoch)
	assert.NotNil(t, opts.idGenerator)
	assert.Equal(t, "id-1", opts.idGenerator())
}

func TestHelpersAndKeys(t *testing.T) {
	q := newTestQueue()

	assert.True(t, validIdentifier("valid_name_1"))
	assert.False(t, validIdentifier("1invalid"))
	assert.False(t, validIdentifier("bad-name"))

	assert.Equal(t, `"infra"."jobx_delay_jobs"`, q.fullTableName())
	assert.Equal(t, `"infra"."jobx_delay_jobs"`, q.tableRegclass())
	assert.Equal(t, `"jobx_delay_jobs_pending_idx"`, q.indexName("pending_idx"))
	assert.Equal(t, "1 milliseconds", durationInterval(0))
	assert.Equal(t, "250 milliseconds", durationInterval(250*time.Millisecond))
	assert.True(t, q.inHotWindow(time.Unix(100, 0).UTC(), time.Unix(100, 0).UTC().Add(30*time.Minute)))
	assert.False(t, q.inHotWindow(time.Unix(100, 0).UTC(), time.Unix(100, 0).UTC().Add(2*time.Hour)))

	id := "job-42"
	expectedShard := func() int {
		h := fnv.New32a()
		_, _ = h.Write([]byte(id))
		return int(h.Sum32() % uint32(q.opts.shardNum))
	}()

	assert.Equal(t, expectedShard, q.shard(id))
	assert.Equal(t, "jobx:{test_queue:3}:ready", q.readyKey(3))
	assert.Equal(t, "jobx:{test_queue:3}:pending", q.pendingKey(3))
	assert.Equal(t, "jobx:{test_queue:3}:payload:job-42", q.payloadKey(3, id))

	ts := time.Date(2026, 4, 9, 10, 11, 12, 123456789, time.FixedZone("UTC+8", 8*3600))
	assert.Equal(t, time.Date(2026, 4, 9, 2, 11, 12, 123000000, time.UTC), normalizeTimestamp(ts))
	assert.True(t, normalizeTimestamp(time.Time{}).IsZero())
}

func TestEnqueueBatchGuards(t *testing.T) {
	ctx := context.Background()

	t.Run("closed queue", func(t *testing.T) {
		q := newTestQueue()
		q.closed.Store(true)

		ids, err := q.EnqueueBatch(ctx, []Job{{RunAt: time.Now()}})
		require.ErrorIs(t, err, ErrQueueClosed)
		assert.Nil(t, ids)
	})

	t.Run("empty batch", func(t *testing.T) {
		q := newTestQueue()

		ids, err := q.EnqueueBatch(ctx, nil)
		require.NoError(t, err)
		assert.Nil(t, ids)
	})

	t.Run("zero run at", func(t *testing.T) {
		q := newTestQueue()

		ids, err := q.EnqueueBatch(ctx, []Job{{}})
		require.ErrorIs(t, err, ErrInvalidJob)
		assert.Nil(t, ids)
		assert.Contains(t, err.Error(), "run_at is zero")
	})

	t.Run("missing id source", func(t *testing.T) {
		q := newTestQueue()
		q.idGenerator = func() string { return "" }

		ids, err := q.EnqueueBatch(ctx, []Job{{RunAt: time.Now()}})
		require.ErrorIs(t, err, ErrMissingIDSource)
		assert.Nil(t, ids)
	})
}

func TestStartDeleteAndShutdownGuards(t *testing.T) {
	ctx := context.Background()

	t.Run("start missing handler", func(t *testing.T) {
		q := newTestQueue()
		err := q.Start(ctx, nil)
		require.ErrorIs(t, err, ErrMissingHandler)
	})

	t.Run("start closed queue", func(t *testing.T) {
		q := newTestQueue()
		q.closed.Store(true)
		err := q.Start(ctx, func(context.Context, []Job) []string { return nil })
		require.ErrorIs(t, err, ErrQueueClosed)
	})

	t.Run("start already started", func(t *testing.T) {
		q := newTestQueue()
		q.started.Store(true)
		err := q.Start(ctx, func(context.Context, []Job) []string { return nil })
		require.ErrorIs(t, err, ErrAlreadyStarted)
	})

	t.Run("delete empty id", func(t *testing.T) {
		q := newTestQueue()
		err := q.Delete(ctx, "")
		require.ErrorIs(t, err, ErrInvalidJob)
		assert.Contains(t, err.Error(), "empty id")
	})

	t.Run("delete batch empty id", func(t *testing.T) {
		q := newTestQueue()
		err := q.DeleteBatch(ctx, []string{"job-1", ""})
		require.ErrorIs(t, err, ErrInvalidJob)
		assert.Contains(t, err.Error(), "empty id")
	})

	t.Run("delete batch empty slice", func(t *testing.T) {
		q := newTestQueue()
		require.NoError(t, q.DeleteBatch(ctx, nil))
	})

	t.Run("shutdown without start", func(t *testing.T) {
		q := newTestQueue()
		require.NoError(t, q.Shutdown())
		assert.True(t, q.closed.Load())
	})
}

func TestProcessJobsAndWorkerLoop(t *testing.T) {
	ctx := context.Background()

	t.Run("process jobs recovers panic", func(t *testing.T) {
		q := newTestQueue()
		assert.NotPanics(t, func() {
			q.processJobs(wrappedJobs{ctx: ctx, shard: 1, jobs: []Job{{ID: "job-1"}}}, func(context.Context, []Job) []string {
				panic("boom")
			})
		})
	})

	t.Run("worker loop consumes queue", func(t *testing.T) {
		q := newTestQueue()
		runAt := time.Now().UTC().Truncate(time.Millisecond)
		payload, err := sonic.MarshalString(redisJobEnvelope{
			ID:      "job-1",
			RunAt:   runAt.UnixMilli(),
			Payload: []byte("payload"),
		})
		require.NoError(t, err)
		q.rdb = &fakeCmdable{
			pollIDs:    []interface{}{"job-1"},
			mgetValues: []interface{}{payload},
		}
		runCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		handled := make(chan []Job, 1)
		go q.workerLoop(runCtx, func(_ context.Context, jobs []Job) []string {
			handled <- jobs
			cancel()
			return nil
		})

		want := []Job{{ID: "job-1", RunAt: runAt, Payload: []byte("payload")}}

		select {
		case got := <-handled:
			assert.Equal(t, want, got)
		case <-time.After(2 * time.Second):
			t.Fatal("workerLoop did not process queued jobs")
		}
	})

	t.Run("shutdown waits for in-flight handler", func(t *testing.T) {
		q := newTestQueue()
		runAt := time.Now().UTC().Truncate(time.Millisecond)
		payload, err := sonic.MarshalString(redisJobEnvelope{
			ID:      "job-1",
			RunAt:   runAt.UnixMilli(),
			Payload: []byte("payload"),
		})
		require.NoError(t, err)

		q.rdb = &fakeCmdable{
			pollIDs:    []interface{}{"job-1"},
			mgetValues: []interface{}{payload},
		}

		runCtx, cancel := context.WithCancel(context.Background())
		q.started.Store(true)
		q.runCancel = cancel

		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.workerLoop(runCtx, func(_ context.Context, jobs []Job) []string {
				close(handlerStarted)
				<-releaseHandler
				return nil
			})
		}()

		select {
		case <-handlerStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("handler did not start")
		}

		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- q.Shutdown()
		}()

		select {
		case err := <-shutdownDone:
			t.Fatalf("shutdown returned before handler completed: %v", err)
		case <-time.After(150 * time.Millisecond):
		}

		close(releaseHandler)

		select {
		case err := <-shutdownDone:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not wait for handler completion")
		}
	})
}

func TestRequeueJobsToRedisBestEffort(t *testing.T) {
	q := newTestQueue()
	var attempts int
	rdb := &fakeCmdable{
		evalHook: func(ctx context.Context, script string, _ []string, _ ...interface{}) error {
			if script != requeueJobsScriptSource {
				return nil
			}
			attempts++
			if attempts == 1 {
				return context.Canceled
			}
			return nil
		},
	}
	q.rdb = rdb

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.requeueJobsToRedisBestEffort(canceledCtx, 1, []Job{{ID: "job-1", RunAt: time.UnixMilli(1234).UTC()}})
	require.NoError(t, err)
	require.Len(t, rdb.scriptRuns, 2)
	assert.Equal(t, requeueJobsScriptSource, rdb.scriptRuns[0].script)
	assert.Equal(t, requeueJobsScriptSource, rdb.scriptRuns[1].script)
	assert.Equal(t, 2, attempts)
}

func TestAddJobsToRedisSkipsExpiredJobs(t *testing.T) {
	ctx := context.Background()
	fp := &fakePipeline{}
	rdb := &fakeCmdable{pipeline: fp}
	q := newTestQueue()
	q.rdb = rdb

	now := time.Now()
	jobs := []Job{
		{ID: "keep", RunAt: now.Add(30 * time.Minute), Payload: []byte("payload")},
		{ID: "expired", RunAt: now.Add(-3 * time.Hour), Payload: []byte("old")},
	}

	err := q.addJobsToRedis(ctx, jobs)
	require.NoError(t, err)
	require.Len(t, fp.calls, 1)

	call := fp.calls[0]
	assert.Contains(t, call.script, `redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])`)
	assert.Equal(t, []string{q.readyKey(q.shard("keep")), q.payloadKey(q.shard("keep"), "keep")}, call.keys)
	assert.Equal(t, "keep", call.args[0])
	assert.Equal(t, jobs[0].RunAt.UnixMilli(), call.args[1])

	env, ok := call.args[2].(string)
	require.True(t, ok)

	var decoded redisJobEnvelope
	require.NoError(t, sonic.UnmarshalString(env, &decoded))
	assert.Equal(t, "keep", decoded.ID)
	assert.Equal(t, jobs[0].RunAt.UnixMilli(), decoded.RunAt)
	assert.Equal(t, []byte("payload"), decoded.Payload)

	ttl, ok := call.args[3].(int64)
	require.True(t, ok)
	assert.Positive(t, ttl)
}

func TestPollRedisFiltersInvalidPayloadsAndCleansUp(t *testing.T) {
	ctx := context.Background()
	validRunAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	expiredRunAt := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Millisecond)

	validPayload, err := sonic.MarshalString(redisJobEnvelope{
		ID:      "valid",
		RunAt:   validRunAt.UnixMilli(),
		Payload: []byte("ok"),
	})
	require.NoError(t, err)

	expiredPayload, err := sonic.MarshalString(redisJobEnvelope{
		ID:      "expired",
		RunAt:   expiredRunAt.UnixMilli(),
		Payload: []byte("late"),
	})
	require.NoError(t, err)

	rdb := &fakeCmdable{
		pollIDs: []interface{}{"valid", "missing", "bad-type", "bad-json", "expired"},
		mgetValues: []interface{}{
			validPayload,
			nil,
			123,
			"{bad json",
			expiredPayload,
		},
	}
	q := newTestQueue()
	q.rdb = rdb
	q.opts.retention = 2 * time.Hour

	jobs, err := q.pollRedis(ctx, 2)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, Job{
		ID:      "valid",
		RunAt:   validRunAt,
		Payload: []byte("ok"),
	}, jobs[0])

	require.Len(t, rdb.scriptRuns, 3)
	assert.True(t, strings.Contains(rdb.scriptRuns[0].script, `redis.call("ZRANGE", KEYS[1], "-inf", now, "BYSCORE"`))
	assert.Equal(t, []interface{}{"missing", "bad-type", "bad-json"}, rdb.scriptRuns[1].args)
	assert.Equal(t, []interface{}{"expired"}, rdb.scriptRuns[2].args)
}
