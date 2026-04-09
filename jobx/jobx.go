package jobx

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kanengo/ku/contextx"
	"github.com/kanengo/ku/distributedx"
	dsnowflake "github.com/kanengo/ku/distributedx/snowflake"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNilPostgres     = errors.New("jobx: postgres pool is nil")
	ErrNilRedis        = errors.New("jobx: redis client is nil")
	ErrInvalidJob      = errors.New("jobx: invalid job")
	ErrInvalidOption   = errors.New("jobx: invalid option")
	ErrQueueClosed     = errors.New("jobx: delay queue is closed")
	ErrAlreadyStarted  = errors.New("jobx: delay queue already started")
	ErrMissingHandler  = errors.New("jobx: handler is nil")
	ErrMissingIDSource = errors.New("jobx: id generator returned empty id")
)

const (
	statusPending   = "pending"
	statusSucceeded = "succeeded"
	statusCancelled = "cancelled"

	timestampPrecision = time.Millisecond

	defaultQueueName       = "default"
	defaultSchema          = "infra"
	defaultTable           = "jobx_delay_jobs"
	defaultSnowflakeTable  = "jobx_snowflake_workers"
	defaultHotWindow       = time.Hour
	defaultRetention       = 7 * 24 * time.Hour
	defaultChunkInterval   = 24 * time.Hour
	defaultPromoteInterval = time.Minute
	defaultRefreshInterval = 5 * time.Minute
	defaultPollingInterval = time.Second
	defaultLeaseTimeout    = 30 * time.Second
	defaultBatchSize       = 100
	defaultShardNum        = 16
	defaultConcurrency     = 4
	defaultQueueDepth      = 100
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Job struct {
	ID      string
	RunAt   time.Time
	Payload []byte
}

type Handler func(context.Context, []Job) []string

type IDGenerator func() string

type Option func(*options)

type options struct {
	queue                  string
	schema                 string
	table                  string
	hotWindow              time.Duration
	retention              time.Duration
	chunkInterval          time.Duration
	promoteInterval        time.Duration
	promoteRefreshInterval time.Duration
	pollingInterval        time.Duration
	leaseTimeout           time.Duration
	batchSize              int
	shardNum               int
	concurrency            int
	queueDepth             int
	idGenerator            IDGenerator
	snowflakeDSN           string
	snowflakeTable         string
	snowflakeEpoch         int64
}

var defaultOptions = options{
	queue:                  defaultQueueName,
	schema:                 defaultSchema,
	table:                  defaultTable,
	hotWindow:              defaultHotWindow,
	retention:              defaultRetention,
	chunkInterval:          defaultChunkInterval,
	promoteInterval:        defaultPromoteInterval,
	promoteRefreshInterval: defaultRefreshInterval,
	pollingInterval:        defaultPollingInterval,
	leaseTimeout:           defaultLeaseTimeout,
	batchSize:              defaultBatchSize,
	shardNum:               defaultShardNum,
	concurrency:            defaultConcurrency,
	queueDepth:             defaultQueueDepth,
	snowflakeTable:         defaultSnowflakeTable,
}

func WithQueue(queue string) Option {
	return func(o *options) {
		if queue != "" {
			o.queue = queue
		}
	}
}

func WithSchema(schema string) Option {
	return func(o *options) {
		if schema != "" {
			o.schema = schema
		}
	}
}

func WithTable(table string) Option {
	return func(o *options) {
		if table != "" {
			o.table = table
		}
	}
}

func WithHotWindow(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.hotWindow = d
		}
	}
}

func WithRetention(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.retention = d
		}
	}
}

func WithChunkInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.chunkInterval = d
		}
	}
}

func WithPromoteInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.promoteInterval = d
		}
	}
}

func WithPromoteRefreshInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.promoteRefreshInterval = d
		}
	}
}

func WithPollingInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.pollingInterval = d
		}
	}
}

func WithLeaseTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.leaseTimeout = d
		}
	}
}

func WithBatchSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

func WithShardNum(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.shardNum = n
		}
	}
}

func WithConcurrency(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

func WithQueueDepth(n int) Option {
	return func(o *options) {
		if n >= 0 {
			o.queueDepth = n
		}
	}
}

func WithSnowflakeDSN(dsn string) Option {
	return func(o *options) {
		o.snowflakeDSN = dsn
	}
}

func WithSnowflakeTable(table string) Option {
	return func(o *options) {
		if table != "" {
			o.snowflakeTable = table
		}
	}
}

func WithSnowflakeEpoch(epoch int64) Option {
	return func(o *options) {
		o.snowflakeEpoch = epoch
	}
}

func WithIDGenerator(fn IDGenerator) Option {
	return func(o *options) {
		o.idGenerator = fn
	}
}

type DelayQueue struct {
	pg  *pgxpool.Pool
	rdb redis.Cmdable

	opts options

	idGenerator IDGenerator
	snowflake   *dsnowflake.Snowflake

	queueCh chan wrappedJobs

	startMu    sync.Mutex
	shutdownMu sync.Mutex
	acceptMu   sync.RWMutex
	execMu     sync.RWMutex
	runCancel  context.CancelFunc
	wg         sync.WaitGroup

	started  atomic.Bool
	closed   atomic.Bool
	shardSeq atomic.Uint64

	closeSnowflakeOnce sync.Once
}

type wrappedJobs struct {
	ctx        context.Context
	shard      int
	leaseUntil time.Time
	jobs       []Job
}

type redisJobEnvelope struct {
	ID      string `json:"id"`
	RunAt   int64  `json:"run_at"`
	Payload []byte `json:"payload"`
}

func NewDelayQueue(ctx context.Context, dsn string, rdb redis.Cmdable, opts ...Option) (*DelayQueue, error) {
	if dsn == "" {
		return nil, ErrNilPostgres
	}
	if rdb == nil {
		return nil, ErrNilRedis
	}
	pg, err := distributedx.GetConn(dsn)
	if err != nil {
		return nil, err
	}
	if err := pg.Ping(ctx); err != nil {
		return nil, err
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	if err := validateOptions(options); err != nil {
		return nil, err
	}

	q := &DelayQueue{
		pg:      pg,
		rdb:     rdb,
		opts:    options,
		queueCh: make(chan wrappedJobs, options.queueDepth),
	}

	if err := q.ensureSchema(ctx); err != nil {
		return nil, err
	}

	switch {
	case options.idGenerator != nil:
		q.idGenerator = options.idGenerator
	case options.snowflakeDSN != "":
		sf, err := dsnowflake.New(ctx, dsnowflake.Config{
			DSN:       options.snowflakeDSN,
			Schema:    options.schema,
			TableName: options.snowflakeTable,
			Epoch:     options.snowflakeEpoch,
		})
		if err != nil {
			return nil, err
		}
		q.snowflake = sf
		q.idGenerator = func() string {
			return strconv.FormatInt(sf.Generate(), 10)
		}
	default:
		q.idGenerator = func() string {
			id, err := uuid.NewV7()
			if err != nil {
				return ""
			}
			return id.String()
		}
	}

	q.shardSeq.Store(rand.Uint64() % uint64(options.shardNum))

	return q, nil
}

func validateOptions(o options) error {
	if !validIdentifier(o.schema) {
		return fmt.Errorf("%w: invalid schema %q", ErrInvalidOption, o.schema)
	}
	if !validIdentifier(o.table) {
		return fmt.Errorf("%w: invalid table %q", ErrInvalidOption, o.table)
	}
	if o.snowflakeTable != "" && !validIdentifier(o.snowflakeTable) {
		return fmt.Errorf("%w: invalid snowflake table %q", ErrInvalidOption, o.snowflakeTable)
	}
	if o.hotWindow <= 0 || o.retention <= 0 || o.chunkInterval <= 0 ||
		o.promoteInterval <= 0 || o.promoteRefreshInterval <= 0 ||
		o.pollingInterval <= 0 || o.leaseTimeout <= 0 {
		return fmt.Errorf("%w: duration options must be positive", ErrInvalidOption)
	}
	if o.batchSize <= 0 || o.shardNum <= 0 || o.concurrency <= 0 || o.queueDepth < 0 {
		return fmt.Errorf("%w: numeric options out of range", ErrInvalidOption)
	}
	return nil
}

func validIdentifier(s string) bool {
	return identifierPattern.MatchString(s)
}

func (q *DelayQueue) fullTableName() string {
	return fmt.Sprintf(`"%s"."%s"`, q.opts.schema, q.opts.table)
}

func (q *DelayQueue) tableRegclass() string {
	return q.fullTableName()
}

func (q *DelayQueue) indexName(suffix string) string {
	return fmt.Sprintf(`"%s_%s"`, q.opts.table, suffix)
}

func durationInterval(d time.Duration) string {
	ms := d.Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	return fmt.Sprintf("%d milliseconds", ms)
}

func normalizeTimestamp(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC().Truncate(timestampPrecision)
}

func nowTimestamp() time.Time {
	return normalizeTimestamp(time.Now())
}

func (q *DelayQueue) ensureSchema(ctx context.Context) error {
	if _, err := q.pg.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, q.opts.schema)); err != nil {
		return err
	}
	if _, err := q.pg.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb`); err != nil {
		return err
	}

	createTable := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	queue TEXT NOT NULL,
	id TEXT NOT NULL,
	run_at TIMESTAMPTZ NOT NULL,
	payload BYTEA NOT NULL,
	status TEXT NOT NULL,
	promoted_at TIMESTAMPTZ,
	lease_until TIMESTAMPTZ,
	attempts BIGINT NOT NULL DEFAULT 0,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
)`, q.fullTableName())
	if _, err := q.pg.Exec(ctx, createTable); err != nil {
		return err
	}
	if _, err := q.pg.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ`, q.fullTableName())); err != nil {
		return err
	}

	if _, err := q.pg.Exec(ctx,
		`SELECT create_hypertable($1::regclass, 'run_at', chunk_time_interval => $2::interval, if_not_exists => TRUE)`,
		q.tableRegclass(), durationInterval(q.opts.chunkInterval)); err != nil {
		return err
	}

	if _, err := q.pg.Exec(ctx,
		`SELECT add_retention_policy($1::regclass, $2::interval, if_not_exists => TRUE)`,
		q.tableRegclass(), durationInterval(q.opts.retention)); err != nil {
		return err
	}

	statements := []string{
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (queue, run_at, id)`, q.indexName("queue_run_id_uidx"), q.fullTableName()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (queue, run_at) WHERE status = '%s'`, q.indexName("pending_run_idx"), q.fullTableName(), statusPending),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (queue, promoted_at, run_at) WHERE status = '%s'`, q.indexName("promote_idx"), q.fullTableName(), statusPending),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (queue, id, run_at)`, q.indexName("queue_id_run_idx"), q.fullTableName()),
	}
	for _, stmt := range statements {
		if _, err := q.pg.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}

func (q *DelayQueue) Enqueue(ctx context.Context, job Job) (string, error) {
	ids, err := q.EnqueueBatch(ctx, []Job{job})
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", ErrInvalidJob
	}
	return ids[0], nil
}

func (q *DelayQueue) EnqueueBatch(ctx context.Context, jobs []Job) ([]string, error) {
	q.acceptMu.RLock()
	defer q.acceptMu.RUnlock()

	if q.closed.Load() {
		return nil, ErrQueueClosed
	}
	if len(jobs) == 0 {
		return nil, nil
	}

	now := nowTimestamp()
	normalized := make([]Job, len(jobs))
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		if job.RunAt.IsZero() {
			return nil, fmt.Errorf("%w: run_at is zero", ErrInvalidJob)
		}
		if job.ID == "" {
			job.ID = q.idGenerator()
			if job.ID == "" {
				return nil, ErrMissingIDSource
			}
		}
		if job.Payload == nil {
			job.Payload = []byte{}
		}
		job.RunAt = normalizeTimestamp(job.RunAt)
		normalized[i] = job
		ids[i] = job.ID
	}

	rows := make([][]any, len(normalized))
	for i, job := range normalized {
		rows[i] = []any{
			q.opts.queue,
			job.ID,
			job.RunAt,
			job.Payload,
			statusPending,
			now,
			now,
		}
	}

	inserted, err := q.pg.CopyFrom(
		ctx,
		pgx.Identifier{q.opts.schema, q.opts.table},
		[]string{"queue", "id", "run_at", "payload", "status", "created_at", "updated_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return nil, err
	}
	if inserted != int64(len(rows)) {
		return nil, fmt.Errorf("jobx: copied %d rows, expected %d", inserted, len(rows))
	}

	hotJobs := make([]Job, 0, len(normalized))
	for _, job := range normalized {
		if q.inHotWindow(now, job.RunAt) {
			hotJobs = append(hotJobs, job)
		}
	}
	if err := q.addJobsToRedis(ctx, hotJobs); err != nil {
		contextx.Logger(ctx).Warn("[DelayQueue] enqueue redis hot write failed", "err", err, "queue", q.opts.queue)
	}

	return ids, nil
}

func (q *DelayQueue) inHotWindow(now time.Time, runAt time.Time) bool {
	return !runAt.After(now.Add(q.opts.hotWindow)) && !runAt.Before(now.Add(-q.opts.retention))
}

func (q *DelayQueue) Start(ctx context.Context, handler Handler) error {
	if handler == nil {
		return ErrMissingHandler
	}
	if q.closed.Load() {
		return ErrQueueClosed
	}
	if !q.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	runCtx, cancel := context.WithCancel(ctx)
	q.startMu.Lock()
	q.runCancel = cancel
	q.startMu.Unlock()

	q.wg.Go(func() {
		q.promoteLoop(runCtx)
	})

	for shard := range q.opts.shardNum {
		q.wg.Add(1)
		go func(shard int) {
			defer q.wg.Done()
			q.reclaimLoop(runCtx, shard)
		}(shard)
	}

	for range q.opts.concurrency {
		q.wg.Go(func() {
			q.workerLoop(runCtx, handler)
		})
	}

	return nil
}

func (q *DelayQueue) Shutdown() error {
	q.shutdownMu.Lock()
	defer q.shutdownMu.Unlock()

	q.closed.Store(true)

	q.startMu.Lock()
	cancel := q.runCancel
	q.runCancel = nil
	q.startMu.Unlock()
	if cancel != nil {
		cancel()
	}

	q.acceptMu.Lock()
	q.acceptMu.Unlock()

	if q.started.CompareAndSwap(true, false) {
		q.execMu.Lock()
		q.execMu.Unlock()
		q.wg.Wait()
	}

	q.closeSnowflake()
	return nil
}

func (q *DelayQueue) closeSnowflake() {
	q.closeSnowflakeOnce.Do(func() {
		if q.snowflake != nil {
			q.snowflake.Close()
		}
	})
}

func (q *DelayQueue) Delete(ctx context.Context, id string) error {
	return q.DeleteBatch(ctx, []string{id})
}

func (q *DelayQueue) DeleteBatch(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	uniqueIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			return fmt.Errorf("%w: empty id", ErrInvalidJob)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniqueIDs = append(uniqueIDs, id)
	}

	now := nowTimestamp()
	query := fmt.Sprintf(`
UPDATE %s
SET status = $1, updated_at = $2, lease_until = NULL
WHERE queue = $3 AND id = ANY($4::text[]) AND status = $5`, q.fullTableName())
	if _, err := q.pg.Exec(ctx, query, statusCancelled, now, q.opts.queue, uniqueIDs, statusPending); err != nil {
		return err
	}

	idsByShard := make(map[int][]string, len(uniqueIDs))
	for _, id := range uniqueIDs {
		shard := q.shard(id)
		idsByShard[shard] = append(idsByShard[shard], id)
	}

	for shard, shardIDs := range idsByShard {
		if err := q.removeIDsFromRedis(ctx, shard, shardIDs); err != nil {
			return err
		}
	}

	return nil
}

func (q *DelayQueue) promoteLoop(ctx context.Context) {
	q.promoteUntilDrained(ctx)

	ticker := time.NewTicker(q.opts.promoteInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.promoteUntilDrained(ctx)
		}
	}
}

func (q *DelayQueue) promoteUntilDrained(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := q.promoteOnce(ctx)
		if err != nil {
			contextx.Logger(ctx).Warn("[DelayQueue] promote failed", "err", err, "queue", q.opts.queue)
			return
		}
		if n < q.opts.batchSize {
			return
		}
	}
}

func (q *DelayQueue) promoteOnce(ctx context.Context) (int, error) {
	now := nowTimestamp()
	horizon := now.Add(q.opts.hotWindow)
	cutoff := now.Add(-q.opts.retention)
	refreshBefore := now.Add(-q.opts.promoteRefreshInterval)

	tx, err := q.pg.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf(`
SELECT id, run_at, payload
FROM %s
WHERE queue = $1
  AND status = $2
  AND run_at <= $3
  AND run_at >= $4
  AND (lease_until IS NULL OR lease_until < $5)
  AND (promoted_at IS NULL OR promoted_at < $6)
ORDER BY run_at
LIMIT $7
FOR UPDATE SKIP LOCKED`, q.fullTableName())
	rows, err := tx.Query(ctx, query, q.opts.queue, statusPending, horizon, cutoff, now, refreshBefore, q.opts.batchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	jobs := make([]Job, 0, q.opts.batchSize)
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.RunAt, &job.Payload); err != nil {
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if err := q.addJobsToRedis(ctx, jobs); err != nil {
		return 0, err
	}

	ids := make([]string, 0, len(jobs))
	runAts := make([]time.Time, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.ID)
		runAts = append(runAts, normalizeTimestamp(job.RunAt))
	}

	updateSQL := fmt.Sprintf(`
WITH candidate(id, run_at) AS (
	SELECT * FROM unnest($3::text[], $4::timestamptz[])
)
UPDATE %s t
SET promoted_at = $1, updated_at = $1
FROM candidate c
WHERE t.queue = $2
  AND t.id = c.id
  AND t.run_at = c.run_at
  AND t.status = $5
RETURNING t.id`, q.fullTableName())
	updatedRows, err := tx.Query(ctx, updateSQL, now, q.opts.queue, ids, runAts, statusPending)
	if err != nil {
		return 0, err
	}
	defer updatedRows.Close()

	updated := 0
	for updatedRows.Next() {
		updated++
	}
	if err := updatedRows.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return updated, nil
}

func (q *DelayQueue) nextShardStart() int {
	return int(q.shardSeq.Add(1)-1) % q.opts.shardNum
}

func (q *DelayQueue) pollOneBatch(ctx context.Context) (wrappedJobs, bool) {
	start := q.nextShardStart()
	for i := range q.opts.shardNum {
		shard := (start + i) % q.opts.shardNum
		jobs, err := q.pollRedis(ctx, shard)
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				contextx.Logger(ctx).Warn("[DelayQueue] poll redis failed", "err", err, "queue", q.opts.queue, "shard", shard)
			}
			continue
		}
		if len(jobs) == 0 {
			continue
		}
		return wrappedJobs{ctx: ctx, shard: shard, jobs: jobs}, true
	}

	return wrappedJobs{}, false
}

func (q *DelayQueue) claimJobs(ctx context.Context, jobs []Job) ([]Job, []string, time.Time, error) {
	if len(jobs) == 0 {
		return nil, nil, time.Time{}, nil
	}
	if q.pg == nil {
		return jobs, nil, time.Time{}, nil
	}

	now := nowTimestamp()
	leaseUntil := normalizeTimestamp(now.Add(q.opts.leaseTimeout))
	ids := make([]string, len(jobs))
	runAts := make([]time.Time, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
		runAts[i] = normalizeTimestamp(job.RunAt)
	}

	query := fmt.Sprintf(`
WITH candidate(id, run_at) AS (
	SELECT * FROM unnest($1::text[], $2::timestamptz[])
)
UPDATE %s t
SET lease_until = $3, updated_at = $4, attempts = t.attempts + 1
FROM candidate c
WHERE t.queue = $5
  AND t.id = c.id
  AND t.run_at = c.run_at
  AND t.status = $6
  AND (t.lease_until IS NULL OR t.lease_until < $4)
RETURNING t.id, t.run_at, t.payload`, q.fullTableName())

	rows, err := q.pg.Query(ctx, query, ids, runAts, leaseUntil, now, q.opts.queue, statusPending)
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	defer rows.Close()

	claimed := make([]Job, 0, len(jobs))
	claimedSet := make(map[string]struct{}, len(jobs))
	jobIdentityKey := func(id string, runAt time.Time) string {
		return id + "/" + strconv.FormatInt(normalizeTimestamp(runAt).UnixMilli(), 10)
	}
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.RunAt, &job.Payload); err != nil {
			return nil, nil, time.Time{}, err
		}
		claimed = append(claimed, job)
		claimedSet[jobIdentityKey(job.ID, job.RunAt)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, time.Time{}, err
	}

	if len(claimed) == len(jobs) {
		return claimed, nil, leaseUntil, nil
	}

	residualJobs := make([]Job, 0, len(jobs)-len(claimed))
	for _, job := range jobs {
		if _, ok := claimedSet[jobIdentityKey(job.ID, job.RunAt)]; ok {
			continue
		}
		residualJobs = append(residualJobs, job)
	}

	invalidIDs, err := q.findInvalidJobs(ctx, residualJobs)
	if err != nil {
		return nil, nil, time.Time{}, err
	}

	return claimed, invalidIDs, leaseUntil, nil
}

func (q *DelayQueue) workerLoop(ctx context.Context, handler Handler) {
	for {
		if ctx.Err() != nil || q.closed.Load() {
			return
		}

		msg, ok := q.pollOneBatch(ctx)
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(q.opts.pollingInterval):
				continue
			}
		}

		claimedJobs, invalidIDs, leaseUntil, err := q.claimJobs(ctx, msg.jobs)
		if err != nil {
			contextx.Logger(ctx).Warn("[DelayQueue] claim jobs failed", "err", err, "queue", q.opts.queue, "shard", msg.shard)
			if requeueErr := q.requeueJobsToRedis(ctx, msg.shard, msg.jobs); requeueErr != nil {
				contextx.Logger(ctx).Warn("[DelayQueue] requeue after claim failure failed", "err", requeueErr, "queue", q.opts.queue, "shard", msg.shard)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(q.opts.pollingInterval):
				continue
			}
		}
		if len(invalidIDs) > 0 {
			if err := q.removeIDsFromRedis(ctx, msg.shard, invalidIDs); err != nil {
				contextx.Logger(ctx).Warn("[DelayQueue] cleanup invalid claimed jobs failed", "err", err, "queue", q.opts.queue, "shard", msg.shard)
			}
		}
		if len(claimedJobs) == 0 {
			continue
		}
		msg.jobs = claimedJobs
		msg.leaseUntil = leaseUntil

		q.execMu.RLock()
		if ctx.Err() != nil || q.closed.Load() {
			q.execMu.RUnlock()
			return
		}
		q.processJobs(msg, handler)
		q.execMu.RUnlock()
	}
}

func (q *DelayQueue) processJobs(msg wrappedJobs, handler Handler) {
	defer func() {
		if r := recover(); r != nil {
			contextx.Logger(msg.ctx).Error("[DelayQueue] handler panic", "err", r, "queue", q.opts.queue, "shard", msg.shard)
		}
	}()

	ackIDs := handler(msg.ctx, msg.jobs)
	if len(ackIDs) == 0 {
		return
	}
	if err := q.ackJobs(msg.ctx, msg.shard, msg.jobs, ackIDs, msg.leaseUntil); err != nil {
		contextx.Logger(msg.ctx).Error("[DelayQueue] ack failed", "err", err, "queue", q.opts.queue, "shard", msg.shard)
	}
}

func (q *DelayQueue) ackJobs(ctx context.Context, shard int, jobs []Job, ackIDs []string, leaseUntil time.Time) error {
	jobByID := make(map[string]Job, len(jobs))
	for _, job := range jobs {
		jobByID[job.ID] = job
	}

	now := nowTimestamp()
	ids := make([]string, 0, len(ackIDs))
	runAts := make([]time.Time, 0, len(ackIDs))
	for _, id := range ackIDs {
		job, ok := jobByID[id]
		if !ok {
			continue
		}
		ids = append(ids, id)
		runAts = append(runAts, normalizeTimestamp(job.RunAt))
	}
	if len(ids) == 0 {
		return nil
	}

	updateSQL := fmt.Sprintf(`
WITH candidate(id, run_at) AS (
	SELECT * FROM unnest($4::text[], $5::timestamptz[])
)
UPDATE %s t
SET status = $1, updated_at = $2, lease_until = NULL
FROM candidate c
WHERE t.queue = $3
  AND t.id = c.id
  AND t.run_at = c.run_at
  AND t.status = $6
  AND t.lease_until = $7
RETURNING t.id`, q.fullTableName())

	rows, err := q.pg.Query(ctx, updateSQL, statusSucceeded, now, q.opts.queue, ids, runAts, statusPending, normalizeTimestamp(leaseUntil))
	if err != nil {
		return err
	}
	defer rows.Close()

	cleanupIDs := make([]string, 0, len(ids))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		cleanupIDs = append(cleanupIDs, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return q.removeIDsFromRedis(ctx, shard, cleanupIDs)
}

func (q *DelayQueue) reclaimLoop(ctx context.Context, shard int) {
	q.reclaimOnce(ctx, shard)

	ticker := time.NewTicker(q.opts.leaseTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.reclaimOnce(ctx, shard)
		}
	}
}

const addJobScriptSource = `
local ttl = tonumber(ARGV[4])
if ttl <= 0 then
	return 0
end
redis.call("SET", KEYS[2], ARGV[3], "PX", ttl)
redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])
return 1
`

var addJobScript = redis.NewScript(addJobScriptSource)

func (q *DelayQueue) addJobsToRedis(ctx context.Context, jobs []Job) error {
	if len(jobs) == 0 {
		return nil
	}

	pl := q.rdb.Pipeline()
	for _, job := range jobs {
		retainUntil := job.RunAt.Add(q.opts.retention)
		ttl := time.Until(retainUntil)
		if ttl <= 0 {
			continue
		}
		env, err := sonic.MarshalString(redisJobEnvelope{
			ID:      job.ID,
			RunAt:   normalizeTimestamp(job.RunAt).UnixMilli(),
			Payload: job.Payload,
		})
		if err != nil {
			return err
		}
		shard := q.shard(job.ID)
		// Pipeline does not transparently recover from NOSCRIPT, so use EVAL directly here.
		addJobScript.Eval(ctx, pl, []string{
			q.readyKey(shard),
			q.payloadKey(shard, job.ID),
		}, job.ID, normalizeTimestamp(job.RunAt).UnixMilli(), env, ttl.Milliseconds())
	}
	_, err := pl.Exec(ctx)
	return err
}

var pollJobsScript = redis.NewScript(`
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
`)

func (q *DelayQueue) pollRedis(ctx context.Context, shard int) ([]Job, error) {
	now := nowTimestamp()
	ids, err := pollJobsScript.Run(ctx, q.rdb, []string{
		q.readyKey(shard),
		q.pendingKey(shard),
	}, q.opts.batchSize, now.UnixMilli(), now.Add(q.opts.leaseTimeout).UnixMilli()).StringSlice()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	payloadKeys := make([]string, 0, len(ids))
	for _, id := range ids {
		payloadKeys = append(payloadKeys, q.payloadKey(shard, id))
	}

	values, err := q.rdb.MGet(ctx, payloadKeys...).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]Job, 0, len(values))
	missing := make([]string, 0)
	expired := make([]string, 0)
	for i, raw := range values {
		if raw == nil {
			missing = append(missing, ids[i])
			continue
		}
		payload, ok := raw.(string)
		if !ok {
			missing = append(missing, ids[i])
			continue
		}

		var env redisJobEnvelope
		if err := sonic.UnmarshalString(payload, &env); err != nil {
			missing = append(missing, ids[i])
			continue
		}
		if env.ID != ids[i] {
			missing = append(missing, ids[i])
			continue
		}

		runAt := normalizeTimestamp(time.UnixMilli(env.RunAt))
		if now.After(runAt.Add(q.opts.retention)) {
			expired = append(expired, ids[i])
			continue
		}

		jobs = append(jobs, Job{
			ID:      env.ID,
			RunAt:   runAt,
			Payload: env.Payload,
		})
	}

	if len(missing) > 0 {
		if err := q.removeIDsFromRedis(ctx, shard, missing); err != nil {
			contextx.Logger(ctx).Warn("[DelayQueue] cleanup missing redis jobs failed", "err", err, "queue", q.opts.queue, "shard", shard)
		}
	}
	if len(expired) > 0 {
		if err := q.removeIDsFromRedis(ctx, shard, expired); err != nil {
			contextx.Logger(ctx).Warn("[DelayQueue] cleanup expired redis jobs failed", "err", err, "queue", q.opts.queue, "shard", shard)
		}
	}

	return jobs, nil
}

const removeIDsScriptSource = `
for i, id in ipairs(ARGV) do
	redis.call("ZREM", KEYS[1], id)
	redis.call("ZREM", KEYS[2], id)
	redis.call("DEL", KEYS[i + 2])
end
return #ARGV
`

var removeIDsScript = redis.NewScript(removeIDsScriptSource)

func (q *DelayQueue) removeIDsFromRedis(ctx context.Context, shard int, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	keys := make([]string, 0, len(ids)+2)
	keys = append(keys, q.readyKey(shard), q.pendingKey(shard))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, q.payloadKey(shard, id))
		args = append(args, id)
	}

	return removeIDsScript.Run(ctx, q.rdb, keys, args...).Err()
}

const reclaimJobsScriptSource = `
local batchSize = tonumber(ARGV[1])
local now = ARGV[2]
local values = redis.call("ZRANGE", KEYS[1], "-inf", now, "BYSCORE", "LIMIT", 0, batchSize)
if #values == 0 then
	return 0
end
redis.call("ZREM", KEYS[1], unpack(values))
for _, id in ipairs(values) do
	redis.call("ZADD", KEYS[2], now, id)
end
return #values
`

var reclaimJobsScript = redis.NewScript(reclaimJobsScriptSource)

const requeueJobsScriptSource = `
for i = 1, #ARGV, 2 do
	local id = ARGV[i]
	local score = ARGV[i + 1]
	redis.call("ZREM", KEYS[1], id)
	redis.call("ZADD", KEYS[2], score, id)
end
return #ARGV / 2
`

var requeueJobsScript = redis.NewScript(requeueJobsScriptSource)

func (q *DelayQueue) reclaimOnce(ctx context.Context, shard int) {
	err := reclaimJobsScript.Run(ctx, q.rdb, []string{
		q.pendingKey(shard),
		q.readyKey(shard),
	}, q.opts.batchSize, nowTimestamp().UnixMilli()).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		contextx.Logger(ctx).Warn("[DelayQueue] reclaim failed", "err", err, "queue", q.opts.queue, "shard", shard)
	}
}

func (q *DelayQueue) requeueJobsToRedis(ctx context.Context, shard int, jobs []Job) error {
	if len(jobs) == 0 {
		return nil
	}

	args := make([]any, 0, len(jobs)*2)
	for _, job := range jobs {
		args = append(args, job.ID, normalizeTimestamp(job.RunAt).UnixMilli())
	}

	return requeueJobsScript.Run(ctx, q.rdb, []string{
		q.pendingKey(shard),
		q.readyKey(shard),
	}, args...).Err()
}

func (q *DelayQueue) findInvalidJobs(ctx context.Context, jobs []Job) ([]string, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if q.pg == nil {
		return nil, nil
	}

	ids := make([]string, len(jobs))
	runAts := make([]time.Time, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
		runAts[i] = normalizeTimestamp(job.RunAt)
	}

	query := fmt.Sprintf(`
WITH candidate(id, run_at) AS (
	SELECT * FROM unnest($2::text[], $3::timestamptz[])
)
SELECT c.id
FROM candidate c
LEFT JOIN %s t
  ON t.queue = $1
 AND t.id = c.id
 AND t.run_at = c.run_at
WHERE t.id IS NULL OR t.status <> $4`, q.fullTableName())

	rows, err := q.pg.Query(ctx, query, q.opts.queue, ids, runAts, statusPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	invalidIDs := make([]string, 0, len(jobs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		invalidIDs = append(invalidIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return invalidIDs, nil
}

func (q *DelayQueue) shard(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(q.opts.shardNum))
}

func (q *DelayQueue) readyKey(shard int) string {
	return fmt.Sprintf("jobx:{%s:%d}:ready", q.opts.queue, shard)
}

func (q *DelayQueue) pendingKey(shard int) string {
	return fmt.Sprintf("jobx:{%s:%d}:pending", q.opts.queue, shard)
}

func (q *DelayQueue) payloadKey(shard int, id string) string {
	return fmt.Sprintf("jobx:{%s:%d}:payload:%s", q.opts.queue, shard, id)
}
