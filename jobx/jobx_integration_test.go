package jobx

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kanengo/ku/internal/conn"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelayQueue_Integration(t *testing.T) {
	const (
		defaultPGDSN          = "host=43.133.32.63 user=soular password=CDdyJmCQ0r0YdBYZ6EWB dbname=soular port=6432 sslmode=disable"
		defaultRedisAddr      = "43.133.32.63:6379"
		defaultRedisPassword  = "NxAAj9edpp3J6zJN7xG6"
		defaultRedisDB        = "1"
		defaultTestTimeout    = 45 * time.Second
		defaultLeaseTimeout   = 400 * time.Millisecond
		defaultPolling        = 80 * time.Millisecond
		defaultPromote        = 80 * time.Millisecond
		defaultPromoteRefresh = 150 * time.Millisecond
	)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	}))
	slog.SetDefault(logger)

	pgDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if pgDSN == "" {
		pgDSN = defaultPGDSN
	}
	redisAddr := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if redisAddr == "" {
		redisAddr = defaultRedisAddr
	}
	redisPassword := os.Getenv("TEST_REDIS_PASSWORD")
	if redisPassword == "" {
		redisPassword = defaultRedisPassword
	}
	redisDBRaw := strings.TrimSpace(os.Getenv("TEST_REDIS_DB"))
	if redisDBRaw == "" {
		redisDBRaw = defaultRedisDB
	}

	if pgDSN == "" || redisAddr == "" {
		t.Skip("Skipping integration test: please fill TEST_PG_DSN and TEST_REDIS_ADDR, or edit the defaults in jobx_integration_test.go")
	}

	redisDB, err := strconv.Atoi(redisDBRaw)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           redisDB,
		MaxRetries:   0,
		DialTimeout:  time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping integration test because redis is unavailable: %v", err)
	}
	defer rdb.Close()

	schema := "infra"
	table := fmt.Sprintf("delay_jobs_it_%d", time.Now().UnixNano())

	t.Cleanup(func() {
		_, _ = rdb.Pipelined(context.Background(), func(p redis.Pipeliner) error { return nil })
	})

	t.Run("EnqueueAndProcess", func(t *testing.T) {
		queue := fmt.Sprintf("queue_enqueue_%d", time.Now().UnixNano())
		q := newIntegrationQueue(t, ctx, pgDSN, rdb, schema, table, queue,
			defaultLeaseTimeout, defaultPolling, defaultPromote, defaultPromoteRefresh)
		defer func() { require.NoError(t, q.Shutdown()) }()

		received := make(chan Job, 8)
		err := q.Start(ctx, func(_ context.Context, jobs []Job) []string {
			ackIDs := make([]string, 0, len(jobs))
			for _, job := range jobs {
				received <- job
				ackIDs = append(ackIDs, job.ID)
			}
			return ackIDs
		})
		require.NoError(t, err)

		jobs := []Job{
			{RunAt: time.Now().Add(1000 * time.Millisecond), Payload: []byte("job-a")},
			{RunAt: time.Now().Add(2000 * time.Millisecond), Payload: []byte("job-b")},
			{RunAt: time.Now().Add(3000 * time.Millisecond), Payload: []byte("job-c")},
		}
		ids, err := q.EnqueueBatch(ctx, jobs)
		require.NoError(t, err)
		require.Len(t, ids, len(jobs))

		got := waitForJobs(t, received, len(jobs), 8*time.Second)
		assert.Len(t, got, len(jobs))
		for _, id := range ids {
			waitForStatus(t, q, id, statusSucceeded, 8*time.Second)
		}
	})

	t.Run("DeletePreventsExecution", func(t *testing.T) {
		queue := fmt.Sprintf("queue_delete_%d", time.Now().UnixNano())
		q := newIntegrationQueue(t, ctx, pgDSN, rdb, schema, table, queue,
			defaultLeaseTimeout, defaultPolling, defaultPromote, defaultPromoteRefresh)
		defer func() { require.NoError(t, q.Shutdown()) }()

		received := make(chan Job, 2)
		err := q.Start(ctx, func(_ context.Context, jobs []Job) []string {
			for _, job := range jobs {
				received <- job
			}
			return nil
		})
		require.NoError(t, err)

		id, err := q.Enqueue(ctx, Job{
			RunAt:   time.Now().Add(500 * time.Millisecond),
			Payload: []byte("should-not-run"),
		})
		require.NoError(t, err)
		require.NoError(t, q.Delete(ctx, id))

		select {
		case job := <-received:
			t.Fatalf("deleted job should not execute, got: %s", job.ID)
		case <-time.After(1500 * time.Millisecond):
		}

		waitForStatus(t, q, id, statusCancelled, 5*time.Second)
	})

	t.Run("LeaseTimeoutReclaimsUnackedJob", func(t *testing.T) {
		queue := fmt.Sprintf("queue_reclaim_%d", time.Now().UnixNano())
		q := newIntegrationQueue(t, ctx, pgDSN, rdb, schema, table, queue,
			defaultLeaseTimeout, defaultPolling, defaultPromote, defaultPromoteRefresh)
		defer func() { require.NoError(t, q.Shutdown()) }()

		var deliveries atomic.Int32
		delivered := make(chan string, 8)
		err := q.Start(ctx, func(_ context.Context, jobs []Job) []string {
			ackIDs := make([]string, 0, len(jobs))
			for _, job := range jobs {
				delivered <- job.ID
				if deliveries.Add(1) >= 2 {
					ackIDs = append(ackIDs, job.ID)
				}
			}
			return ackIDs
		})
		require.NoError(t, err)

		id, err := q.Enqueue(ctx, Job{
			RunAt:   time.Now().Add(100 * time.Millisecond),
			Payload: []byte("retry-once"),
		})
		require.NoError(t, err)

		waitForIDs(t, delivered, 2, 8*time.Second)
		waitForStatus(t, q, id, statusSucceeded, 8*time.Second)
		assert.GreaterOrEqual(t, deliveries.Load(), int32(2))
	})

	t.Cleanup(func() {
		pg := getIntegrationQueueConn(context.Background(), pgDSN)
		if pg == nil {
			t.Log("cleanup drop table skipped: postgres connection unavailable")
		} else if _, err := pg.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, schema, table)); err != nil {
			t.Logf("cleanup drop table failed: %v", err)
		}
		cleanupRedisNamespace(context.Background(), t, rdb, "jobx:{queue_")
	})
}

func newIntegrationQueue(
	t *testing.T,
	ctx context.Context,
	pgDSN string,
	rdb *redis.Client,
	schema string,
	table string,
	queue string,
	leaseTimeout time.Duration,
	polling time.Duration,
	promote time.Duration,
	promoteRefresh time.Duration,
) *DelayQueue {
	t.Helper()

	q, err := NewDelayQueue(ctx, pgDSN, rdb,
		WithSchema(schema),
		WithTable(table),
		WithQueue(queue),
		WithHotWindow(5*time.Second),
		WithRetention(time.Hour),
		WithChunkInterval(time.Hour),
		WithPromoteInterval(promote),
		WithPromoteRefreshInterval(promoteRefresh),
		WithPollingInterval(polling),
		WithLeaseTimeout(leaseTimeout),
		WithBatchSize(8),
		WithShardNum(4),
		WithConcurrency(2),
	)
	require.NoError(t, err)
	return q
}

func waitForJobs(t *testing.T, ch <-chan Job, n int, timeout time.Duration) []Job {
	t.Helper()

	got := make([]Job, 0, n)
	deadline := time.After(timeout)
	for len(got) < n {
		select {
		case job := <-ch:
			got = append(got, job)
		case <-deadline:
			t.Fatalf("timeout waiting for %d jobs, got %d", n, len(got))
		}
	}
	return got
}

func waitForIDs(t *testing.T, ch <-chan string, n int, timeout time.Duration) []string {
	t.Helper()

	got := make([]string, 0, n)
	deadline := time.After(timeout)
	for len(got) < n {
		select {
		case id := <-ch:
			got = append(got, id)
		case <-deadline:
			t.Fatalf("timeout waiting for %d deliveries, got %d", n, len(got))
		}
	}
	return got
}

func waitForStatus(t *testing.T, q *DelayQueue, id string, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := q.pg.QueryRow(context.Background(), fmt.Sprintf(`
SELECT status
FROM %s
WHERE queue = $1 AND id = $2
ORDER BY run_at DESC
LIMIT 1`, q.fullTableName()), q.opts.queue, id).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for job %s status %s", id, want)
}

func getIntegrationQueueConn(ctx context.Context, dsn string) *pgxpool.Pool {
	pg, err := conn.GetPgConn(dsn)
	if err != nil {
		return nil
	}
	if err := pg.Ping(ctx); err != nil {
		return nil
	}
	return pg
}

func cleanupRedisNamespace(ctx context.Context, t *testing.T, rdb *redis.Client, prefix string) {
	t.Helper()

	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			t.Logf("redis cleanup scan failed: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Logf("redis cleanup del failed: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
