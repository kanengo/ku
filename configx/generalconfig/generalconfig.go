package generalconfig

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kanengo/ku/internal/conn"
)

const (
	StatusDisabled = 0
	StatusEnabled  = 1

	defaultSchema                 = "infra"
	defaultTable                  = "general_configs"
	defaultRefreshIntervalSeconds = 60
	defaultQueryTimeout           = 5 * time.Second
)

var (
	ErrNotInitialized = errors.New("generalconfig: default manager is not initialized")
	ErrNilPostgres    = errors.New("generalconfig: postgres dsn is empty")
	ErrEmptyKey       = errors.New("generalconfig: key is empty")
	ErrNotFound       = errors.New("generalconfig: config not found")
	ErrInvalidOption  = errors.New("generalconfig: invalid option")
	ErrClosed         = errors.New("generalconfig: manager is closed")
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var (
	defaultMu      sync.RWMutex
	defaultManager *Manager
)

type Option func(*options)

type options struct {
	schema                 string
	table                  string
	defaultRefreshInterval time.Duration
	queryTimeout           time.Duration
}

var defaultOptions = options{
	schema:                 defaultSchema,
	table:                  defaultTable,
	defaultRefreshInterval: time.Duration(defaultRefreshIntervalSeconds) * time.Second,
	queryTimeout:           defaultQueryTimeout,
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

func WithDefaultRefreshInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.defaultRefreshInterval = d
		}
	}
}

func WithQueryTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.queryTimeout = d
		}
	}
}

type GetOption[T any] func(*getOptions[T])

type getOptions[T any] struct {
	defaultValue T
	hasDefault   bool
	forceRefresh bool
}

func WithDefaultValue[T any](value T) GetOption[T] {
	return func(o *getOptions[T]) {
		o.defaultValue = value
		o.hasDefault = true
	}
}

func WithForceRefresh[T any]() GetOption[T] {
	return func(o *getOptions[T]) {
		o.forceRefresh = true
	}
}

type ListOption func(*listOptions)

type listOptions struct {
	status    int
	hasStatus bool
	limit     int
	offset    int
}

func WithListStatus(status int) ListOption {
	return func(o *listOptions) {
		o.status = status
		o.hasStatus = true
	}
}

func WithListLimit(limit int) ListOption {
	return func(o *listOptions) {
		if limit > 0 {
			o.limit = limit
		}
	}
}

func WithListOffset(offset int) ListOption {
	return func(o *listOptions) {
		if offset > 0 {
			o.offset = offset
		}
	}
}

type WriteOption func(*writeOptions)

type writeOptions struct {
	status          int
	hasStatus       bool
	refreshInterval int
	hasRefresh      bool
}

func WithStatus(status int) WriteOption {
	return func(o *writeOptions) {
		o.status = status
		o.hasStatus = true
	}
}

func WithRefreshInterval(seconds int) WriteOption {
	return func(o *writeOptions) {
		o.refreshInterval = seconds
		o.hasRefresh = true
	}
}

type ConfigRecord struct {
	ID              int64
	Key             string
	Value           string
	Status          int
	RefreshInterval int
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Manager struct {
	pg *pgxpool.Pool

	opts options

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	entries map[string]*entry
	closed  bool

	wg sync.WaitGroup
}

type entry struct {
	key string

	mu              sync.RWMutex
	raw             string
	refreshInterval time.Duration

	ready   chan struct{}
	initErr error

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

func Init(ctx context.Context, dsn string, opts ...Option) error {
	manager, err := NewManager(ctx, dsn, opts...)
	if err != nil {
		return err
	}

	defaultMu.Lock()
	old := defaultManager
	if old != nil {
		_ = old.Close()
	}
	defaultManager = manager
	defaultMu.Unlock()

	return nil
}

func Close() error {
	defaultMu.Lock()
	manager := defaultManager
	defaultManager = nil
	defaultMu.Unlock()

	if manager == nil {
		return nil
	}
	return manager.Close()
}

func Default() *Manager {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	return defaultManager
}

func SetDefault(m *Manager) {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	defaultManager = m
}

func NewManager(ctx context.Context, dsn string, opts ...Option) (*Manager, error) {
	if dsn == "" {
		return nil, ErrNilPostgres
	}

	options, err := applyOptions(defaultOptions, opts...)
	if err != nil {
		return nil, err
	}

	pg, err := conn.GetPgConn(dsn)
	if err != nil {
		return nil, err
	}
	if err := pg.Ping(ctx); err != nil {
		return nil, err
	}

	managerCtx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		pg:      pg,
		opts:    options,
		ctx:     managerCtx,
		cancel:  cancel,
		entries: make(map[string]*entry),
	}

	if err := m.ensureSchema(ctx); err != nil {
		cancel()
		return nil, err
	}

	return m, nil
}

func Get[T any](ctx context.Context, key string, opts ...GetOption[T]) (T, error) {
	options := getOptions[T]{}
	for _, opt := range opts {
		opt(&options)
	}

	manager := Default()
	if manager == nil {
		if options.hasDefault {
			return options.defaultValue, nil
		}
		var zero T
		return zero, ErrNotInitialized
	}

	return getFromManager(ctx, manager, key, options)
}

func List(ctx context.Context, opts ...ListOption) ([]ConfigRecord, error) {
	manager, err := defaultOrErr()
	if err != nil {
		return nil, err
	}
	return manager.List(ctx, opts...)
}

func GetRecord(ctx context.Context, key string) (ConfigRecord, error) {
	manager, err := defaultOrErr()
	if err != nil {
		return ConfigRecord{}, err
	}
	return manager.GetRecord(ctx, key)
}

func Create(ctx context.Context, key string, value any, opts ...WriteOption) (ConfigRecord, error) {
	manager, err := defaultOrErr()
	if err != nil {
		return ConfigRecord{}, err
	}
	return manager.Create(ctx, key, value, opts...)
}

func Update(ctx context.Context, key string, value any, opts ...WriteOption) (ConfigRecord, error) {
	manager, err := defaultOrErr()
	if err != nil {
		return ConfigRecord{}, err
	}
	return manager.Update(ctx, key, value, opts...)
}

func SetStatus(ctx context.Context, key string, status int) error {
	manager, err := defaultOrErr()
	if err != nil {
		return err
	}
	return manager.SetStatus(ctx, key, status)
}

func Delete(ctx context.Context, key string) error {
	manager, err := defaultOrErr()
	if err != nil {
		return err
	}
	return manager.Delete(ctx, key)
}

func GetFrom[T any](ctx context.Context, manager *Manager, key string, opts ...GetOption[T]) (T, error) {
	options := getOptions[T]{}
	for _, opt := range opts {
		opt(&options)
	}
	if manager == nil {
		if options.hasDefault {
			return options.defaultValue, nil
		}
		var zero T
		return zero, ErrNotInitialized
	}
	return getFromManager(ctx, manager, key, options)
}

func (m *Manager) List(ctx context.Context, opts ...ListOption) ([]ConfigRecord, error) {
	if err := m.checkUsable(); err != nil {
		return nil, err
	}

	options := listOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.hasStatus && !validStatus(options.status) {
		return nil, fmt.Errorf("%w: invalid status %d", ErrInvalidOption, options.status)
	}

	query := fmt.Sprintf(`
SELECT id, "key", "value"::text, status, refresh_interval, created_at, updated_at
FROM %s
`, m.fullTableName())
	args := make([]any, 0, 3)
	if options.hasStatus {
		args = append(args, options.status)
		query += fmt.Sprintf(`WHERE status = $%d
`, len(args))
	}
	query += `ORDER BY id DESC`
	if options.limit > 0 {
		args = append(args, options.limit)
		query += fmt.Sprintf(` LIMIT $%d`, len(args))
	}
	if options.offset > 0 {
		args = append(args, options.offset)
		query += fmt.Sprintf(` OFFSET $%d`, len(args))
	}

	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	rows, err := m.pg.Query(qctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]ConfigRecord, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (m *Manager) GetRecord(ctx context.Context, key string) (ConfigRecord, error) {
	if err := m.checkUsable(); err != nil {
		return ConfigRecord{}, err
	}
	return m.fetchRecord(ctx, key, false)
}

func (m *Manager) Create(ctx context.Context, key string, value any, opts ...WriteOption) (ConfigRecord, error) {
	if err := m.checkUsable(); err != nil {
		return ConfigRecord{}, err
	}
	if key == "" {
		return ConfigRecord{}, ErrEmptyKey
	}

	options, err := applyWriteOptions(opts...)
	if err != nil {
		return ConfigRecord{}, err
	}
	status := StatusEnabled
	if options.hasStatus {
		status = options.status
	}
	refreshInterval := int(m.opts.defaultRefreshInterval / time.Second)
	if options.hasRefresh {
		refreshInterval = options.refreshInterval
	}

	raw, err := sonic.MarshalString(value)
	if err != nil {
		return ConfigRecord{}, err
	}

	now := time.Now().UTC()
	query := fmt.Sprintf(`
INSERT INTO %s ("key", "value", status, refresh_interval, created_at, updated_at)
VALUES ($1, $2::jsonb, $3, $4, $5, $5)
RETURNING id, "key", "value"::text, status, refresh_interval, created_at, updated_at
`, m.fullTableName())

	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	record, err := scanRecord(m.pg.QueryRow(qctx, query, key, raw, status, refreshInterval, now))
	if err != nil {
		return ConfigRecord{}, err
	}
	m.syncCachedEntryAfterWrite(ctx, key, record.Status)
	return record, nil
}

func (m *Manager) Update(ctx context.Context, key string, value any, opts ...WriteOption) (ConfigRecord, error) {
	if err := m.checkUsable(); err != nil {
		return ConfigRecord{}, err
	}
	if key == "" {
		return ConfigRecord{}, ErrEmptyKey
	}

	options, err := applyWriteOptions(opts...)
	if err != nil {
		return ConfigRecord{}, err
	}
	raw, err := sonic.MarshalString(value)
	if err != nil {
		return ConfigRecord{}, err
	}

	setParts := []string{`"value" = $2::jsonb`, `updated_at = $3`}
	args := []any{key, raw, time.Now().UTC()}
	if options.hasStatus {
		args = append(args, options.status)
		setParts = append(setParts, fmt.Sprintf(`status = $%d`, len(args)))
	}
	if options.hasRefresh {
		args = append(args, options.refreshInterval)
		setParts = append(setParts, fmt.Sprintf(`refresh_interval = $%d`, len(args)))
	}

	query := fmt.Sprintf(`
UPDATE %s
SET %s
WHERE "key" = $1
RETURNING id, "key", "value"::text, status, refresh_interval, created_at, updated_at
`, m.fullTableName(), joinSQL(setParts, ", "))

	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	record, err := scanRecord(m.pg.QueryRow(qctx, query, args...))
	if err != nil {
		return ConfigRecord{}, mapNoRows(err)
	}
	m.syncCachedEntryAfterWrite(ctx, key, record.Status)
	return record, nil
}

func (m *Manager) SetStatus(ctx context.Context, key string, status int) error {
	if err := m.checkUsable(); err != nil {
		return err
	}
	if key == "" {
		return ErrEmptyKey
	}
	if !validStatus(status) {
		return fmt.Errorf("%w: invalid status %d", ErrInvalidOption, status)
	}

	query := fmt.Sprintf(`
UPDATE %s
SET status = $2, updated_at = $3
WHERE "key" = $1
RETURNING id, "key", "value"::text, status, refresh_interval, created_at, updated_at
`, m.fullTableName())

	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	record, err := scanRecord(m.pg.QueryRow(qctx, query, key, status, time.Now().UTC()))
	if err != nil {
		return mapNoRows(err)
	}
	m.syncCachedEntryAfterWrite(ctx, key, record.Status)
	return nil
}

func (m *Manager) Delete(ctx context.Context, key string) error {
	if err := m.checkUsable(); err != nil {
		return err
	}
	if key == "" {
		return ErrEmptyKey
	}

	query := fmt.Sprintf(`
UPDATE %s
SET status = $2, updated_at = $3
WHERE "key" = $1
`, m.fullTableName())

	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	tag, err := m.pg.Exec(qctx, query, key, StatusDisabled, time.Now().UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	m.stopCachedEntry(key)
	return nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.cancel()

	entries := make([]*entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.entries = make(map[string]*entry)
	for _, e := range entries {
		e.stop()
	}
	m.mu.Unlock()

	m.wg.Wait()
	return nil
}

func defaultOrErr() (*Manager, error) {
	manager := Default()
	if manager == nil {
		return nil, ErrNotInitialized
	}
	return manager, nil
}

func getFromManager[T any](ctx context.Context, manager *Manager, key string, options getOptions[T]) (T, error) {
	raw, err := manager.getRaw(ctx, key, options.forceRefresh)
	if err != nil {
		if options.hasDefault && (errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotInitialized)) {
			return options.defaultValue, nil
		}
		var zero T
		return zero, err
	}

	v, err := decodeRaw[T](raw)
	if err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

func decodeRaw[T any](raw string) (T, error) {
	var v T
	if err := sonic.UnmarshalString(raw, &v); err != nil {
		return v, err
	}
	return v, nil
}

func (m *Manager) getRaw(ctx context.Context, key string, forceRefresh bool) (string, error) {
	if err := m.checkUsable(); err != nil {
		return "", err
	}
	if key == "" {
		return "", ErrEmptyKey
	}
	if ctx == nil {
		ctx = context.Background()
	}

	e, err := m.entryForKey(ctx, key)
	if err != nil {
		return "", err
	}
	if forceRefresh {
		if err := m.refreshEntry(ctx, e); err != nil {
			if errors.Is(err, ErrNotFound) {
				m.stopCachedEntry(key)
			}
			return "", err
		}
	}
	return e.value(), nil
}

func (m *Manager) entryForKey(ctx context.Context, key string) (*entry, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if m.entries == nil {
		m.entries = make(map[string]*entry)
	}
	e, ok := m.entries[key]
	if ok {
		m.mu.Unlock()
		select {
		case <-e.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if e.initErr != nil {
			return nil, e.initErr
		}
		return e, nil
	}

	e = newEntry(key, m.opts.defaultRefreshInterval)
	m.entries[key] = e
	m.mu.Unlock()

	err := m.refreshEntry(ctx, e)
	if err == nil {
		err = m.startRefresh(e)
	}
	e.initErr = err
	close(e.ready)
	if err != nil {
		m.removeEntry(key, e)
		return nil, err
	}
	return e, nil
}

func newEntry(key string, refreshInterval time.Duration) *entry {
	return &entry{
		key:             key,
		refreshInterval: refreshInterval,
		ready:           make(chan struct{}),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
}

func (e *entry) value() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.raw
}

func (e *entry) interval() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.refreshInterval
}

func (e *entry) update(record ConfigRecord, defaultRefreshInterval time.Duration) {
	interval := defaultRefreshInterval
	if record.RefreshInterval > 0 {
		interval = time.Duration(record.RefreshInterval) * time.Second
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.raw = record.Value
	e.refreshInterval = interval
}

func (e *entry) stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
}

func (m *Manager) startRefresh(e *entry) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		e.stop()
		return ErrClosed
	}
	if m.entries[e.key] != e {
		m.mu.Unlock()
		e.stop()
		return ErrNotFound
	}
	m.wg.Add(1)
	m.mu.Unlock()

	go m.refreshLoop(e)
	return nil
}

func (m *Manager) refreshLoop(e *entry) {
	defer m.wg.Done()
	defer close(e.doneCh)

	for {
		timer := time.NewTimer(e.interval())
		select {
		case <-m.ctx.Done():
			stopTimer(timer)
			return
		case <-e.stopCh:
			stopTimer(timer)
			return
		case <-timer.C:
		}

		if err := m.refreshEntry(m.ctx, e); err != nil {
			if errors.Is(err, ErrNotFound) {
				m.removeEntry(e.key, e)
				e.stop()
				return
			}
			slog.Error("generalconfig: refresh config failed", "key", e.key, "err", err)
		}
	}
}

func (m *Manager) refreshEntry(ctx context.Context, e *entry) error {
	record, err := m.fetchRecord(ctx, e.key, true)
	if err != nil {
		return err
	}
	e.update(record, m.opts.defaultRefreshInterval)
	return nil
}

func (m *Manager) syncCachedEntryAfterWrite(ctx context.Context, key string, status int) {
	if status != StatusEnabled {
		m.stopCachedEntry(key)
		return
	}

	m.mu.RLock()
	e := m.entries[key]
	m.mu.RUnlock()
	if e == nil {
		return
	}

	if err := m.refreshEntry(ctx, e); err != nil {
		if errors.Is(err, ErrNotFound) {
			m.stopCachedEntry(key)
			return
		}
		slog.Error("generalconfig: refresh cached config after write failed", "key", key, "err", err)
	}
}

func (m *Manager) stopCachedEntry(key string) {
	m.mu.Lock()
	e := m.entries[key]
	if e != nil {
		delete(m.entries, key)
	}
	m.mu.Unlock()

	if e != nil {
		e.stop()
	}
}

func (m *Manager) removeEntry(key string, target *entry) {
	m.mu.Lock()
	if m.entries[key] == target {
		delete(m.entries, key)
	}
	m.mu.Unlock()
}

func (m *Manager) fetchRecord(ctx context.Context, key string, enabledOnly bool) (ConfigRecord, error) {
	if key == "" {
		return ConfigRecord{}, ErrEmptyKey
	}

	query := fmt.Sprintf(`
SELECT id, "key", "value"::text, status, refresh_interval, created_at, updated_at
FROM %s
WHERE "key" = $1
`, m.fullTableName())
	if enabledOnly {
		query += fmt.Sprintf(`AND status = %d
`, StatusEnabled)
	}
	query += `LIMIT 1`

	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	record, err := scanRecord(m.pg.QueryRow(qctx, query, key))
	if err != nil {
		return ConfigRecord{}, mapNoRows(err)
	}
	return record, nil
}

func (m *Manager) ensureSchema(ctx context.Context) error {
	qctx, cancel := m.queryContext(ctx)
	defer cancel()

	if _, err := m.pg.Exec(qctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, m.opts.schema)); err != nil {
		return err
	}

	createTable := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
	"key" TEXT NOT NULL,
	"value" JSONB NOT NULL,
	status INT NOT NULL DEFAULT %d,
	refresh_interval INT NOT NULL DEFAULT %d,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	CONSTRAINT pkey_id PRIMARY KEY (id),
    CONSTRAINT uk_keyUNIQUE ("key")
)`, m.fullTableName(), StatusEnabled, defaultRefreshIntervalSeconds)
	if _, err := m.pg.Exec(qctx, createTable); err != nil {
		return err
	}

	return nil
}

func (m *Manager) queryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.opts.queryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, m.opts.queryTimeout)
}

func (m *Manager) checkUsable() error {
	if m == nil || m.pg == nil {
		return ErrNotInitialized
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	return nil
}

func (m *Manager) fullTableName() string {
	return fmt.Sprintf(`"%s"."%s"`, m.opts.schema, m.opts.table)
}

func (m *Manager) indexName(suffix string) string {
	return fmt.Sprintf(`%s_%s`, m.opts.table, suffix)
}

func scanRecord(row pgx.Row) (ConfigRecord, error) {
	var record ConfigRecord
	err := row.Scan(
		&record.ID,
		&record.Key,
		&record.Value,
		&record.Status,
		&record.RefreshInterval,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	return record, err
}

func mapNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func applyOptions(base options, opts ...Option) (options, error) {
	o := base
	for _, opt := range opts {
		opt(&o)
	}
	if !validIdentifier(o.schema) {
		return options{}, fmt.Errorf("%w: invalid schema %q", ErrInvalidOption, o.schema)
	}
	if !validIdentifier(o.table) {
		return options{}, fmt.Errorf("%w: invalid table %q", ErrInvalidOption, o.table)
	}
	if o.defaultRefreshInterval <= 0 {
		return options{}, fmt.Errorf("%w: default refresh interval must be positive", ErrInvalidOption)
	}
	if o.queryTimeout < 0 {
		return options{}, fmt.Errorf("%w: query timeout must not be negative", ErrInvalidOption)
	}
	return o, nil
}

func applyWriteOptions(opts ...WriteOption) (writeOptions, error) {
	o := writeOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.hasStatus && !validStatus(o.status) {
		return writeOptions{}, fmt.Errorf("%w: invalid status %d", ErrInvalidOption, o.status)
	}
	if o.hasRefresh && o.refreshInterval < 0 {
		return writeOptions{}, fmt.Errorf("%w: refresh interval must not be negative", ErrInvalidOption)
	}
	return o, nil
}

func validIdentifier(s string) bool {
	return identifierPattern.MatchString(s)
}

func validStatus(status int) bool {
	return status == StatusDisabled || status == StatusEnabled
}

func joinSQL(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, part := range parts[1:] {
		out += sep + part
	}
	return out
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
