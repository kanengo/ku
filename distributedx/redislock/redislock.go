package redislock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTTL          = 30 * time.Second
	defaultSpinInterval = 100 * time.Millisecond
)

var (
	ErrNilRedisClient  = errors.New("redislock: redis client is nil")
	ErrEmptyKey        = errors.New("redislock: key is empty")
	ErrEmptyLockID     = errors.New("redislock: lock id is empty")
	ErrLockNotAcquired = errors.New("redislock: lock not acquired")
	ErrLockNotOwned    = errors.New("redislock: lock not owned")
	ErrLockLost        = errors.New("redislock: lock lost")
	ErrLockReleased    = errors.New("redislock: lock released")
	ErrInvalidOption   = errors.New("redislock: invalid option")
)

type Option func(*options)

type options struct {
	ttl              time.Duration
	spinInterval     time.Duration
	renewInterval    time.Duration
	renewIntervalSet bool
	autoRenew        bool
}

var defaultOptions = options{
	ttl:          defaultTTL,
	spinInterval: defaultSpinInterval,
	autoRenew:    true,
}

func WithTTL(d time.Duration) Option {
	return func(o *options) {
		o.ttl = d
	}
}

func WithSpinInterval(d time.Duration) Option {
	return func(o *options) {
		o.spinInterval = d
	}
}

func WithRenewInterval(d time.Duration) Option {
	return func(o *options) {
		o.renewInterval = d
		o.renewIntervalSet = true
	}
}

func WithAutoRenew(enabled bool) Option {
	return func(o *options) {
		o.autoRenew = enabled
	}
}

type Locker struct {
	rdb  redis.Cmdable
	opts options
}

type Lock struct {
	locker *Locker
	opts   options
	key    string
	id     string

	mu          sync.Mutex
	stopRenewCh chan struct{}
	renewDoneCh chan struct{}
	lostCh      chan error

	stopOnce sync.Once
	lostOnce sync.Once
	released atomic.Bool
}

func New(rdb redis.Cmdable, opts ...Option) (*Locker, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}

	options, err := applyOptions(defaultOptions, opts...)
	if err != nil {
		return nil, err
	}

	return &Locker{
		rdb:  rdb,
		opts: options,
	}, nil
}

func (l *Locker) TryLock(ctx context.Context, key string, opts ...Option) (*Lock, error) {
	lockID, err := newID()
	if err != nil {
		return nil, err
	}
	return l.TryLockWithID(ctx, key, lockID, opts...)
}

func (l *Locker) TryLockWithID(ctx context.Context, key, lockID string, opts ...Option) (*Lock, error) {
	if l == nil || l.rdb == nil {
		return nil, ErrNilRedisClient
	}
	if key == "" {
		return nil, ErrEmptyKey
	}
	if lockID == "" {
		return nil, ErrEmptyLockID
	}

	options, err := applyOptions(l.opts, opts...)
	if err != nil {
		return nil, err
	}

	ok, err := l.acquire(ctx, key, lockID, options.ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}

	lock := &Lock{
		locker:      l,
		opts:        options,
		key:         key,
		id:          lockID,
		stopRenewCh: make(chan struct{}),
		renewDoneCh: make(chan struct{}),
		lostCh:      make(chan error, 1),
	}
	if options.autoRenew {
		go lock.renewLoop()
	} else {
		close(lock.renewDoneCh)
	}

	return lock, nil
}

func (l *Locker) Lock(ctx context.Context, key string, opts ...Option) (*Lock, error) {
	lockID, err := newID()
	if err != nil {
		return nil, err
	}
	return l.LockWithID(ctx, key, lockID, opts...)
}

func (l *Locker) LockWithID(ctx context.Context, key, lockID string, opts ...Option) (*Lock, error) {
	if l == nil || l.rdb == nil {
		return nil, ErrNilRedisClient
	}

	options, err := applyOptions(l.opts, opts...)
	if err != nil {
		return nil, err
	}

	for {
		lock, err := l.TryLockWithID(ctx, key, lockID, withOptions(options))
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, ErrLockNotAcquired) {
			return nil, err
		}

		timer := time.NewTimer(options.spinInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *Locker) Run(ctx context.Context, key string, fn func(context.Context) error, opts ...Option) error {
	lockID, err := newID()
	if err != nil {
		return err
	}
	return l.RunWithID(ctx, key, lockID, fn, opts...)
}

func (l *Locker) RunWithID(ctx context.Context, key, lockID string, fn func(context.Context) error, opts ...Option) error {
	if fn == nil {
		return fmt.Errorf("%w: fn is nil", ErrInvalidOption)
	}

	lock, err := l.LockWithID(ctx, key, lockID, opts...)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	doneCh := make(chan struct{})
	lostErrCh := make(chan error, 1)
	go func() {
		select {
		case err := <-lock.Lost():
			if err != nil {
				lostErrCh <- err
				cancel()
			}
		case <-doneCh:
		}
	}()

	fnErr := fn(runCtx)
	close(doneCh)
	cancel()

	unlockCtx, unlockCancel := context.WithTimeout(context.Background(), lock.opts.commandTimeout())
	defer unlockCancel()
	unlockErr := lock.Unlock(unlockCtx)

	var lostErr error
	select {
	case lostErr = <-lostErrCh:
	default:
	}
	if lostErr != nil {
		if fnErr != nil {
			return errors.Join(lostErr, fnErr)
		}
		return lostErr
	}
	if fnErr != nil {
		if unlockErr != nil && !errors.Is(unlockErr, ErrLockNotOwned) {
			return errors.Join(fnErr, unlockErr)
		}
		return fnErr
	}
	if unlockErr != nil {
		return unlockErr
	}
	return nil
}

func (l *Lock) Unlock(ctx context.Context) error {
	if l == nil || l.locker == nil || l.locker.rdb == nil {
		return ErrNilRedisClient
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.released.Load() {
		return ErrLockReleased
	}

	l.stopRenew()

	n, err := unlockScript.Run(ctx, l.locker.rdb, []string{l.key}, l.id).Int()
	if err != nil {
		return err
	}
	l.released.Store(true)
	if n != 1 {
		return ErrLockNotOwned
	}
	return nil
}

func (l *Lock) Refresh(ctx context.Context) error {
	if l == nil || l.locker == nil || l.locker.rdb == nil {
		return ErrNilRedisClient
	}
	if l.released.Load() {
		return ErrLockReleased
	}

	n, err := refreshScript.Run(ctx, l.locker.rdb, []string{l.key}, l.id, durationMillis(l.opts.ttl)).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrLockLost
	}
	return nil
}

func (l *Lock) Key() string {
	if l == nil {
		return ""
	}
	return l.key
}

func (l *Lock) ID() string {
	if l == nil {
		return ""
	}
	return l.id
}

func (l *Lock) Lost() <-chan error {
	if l == nil {
		ch := make(chan error)
		close(ch)
		return ch
	}
	return l.lostCh
}

func NewID() string {
	id, err := newID()
	if err != nil {
		panic(err)
	}
	return id
}

func newID() (string, error) {
	id, err := gonanoid.New()
	if err != nil {
		return "", fmt.Errorf("redislock: generate lock id: %w", err)
	}
	return id, nil
}

func (l *Locker) acquire(ctx context.Context, key, lockID string, ttl time.Duration) (bool, error) {
	n, err := acquireScript.Run(ctx, l.rdb, []string{key}, lockID, durationMillis(ttl)).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (l *Lock) renewLoop() {
	defer close(l.renewDoneCh)

	ticker := time.NewTicker(l.opts.renewInterval)
	defer ticker.Stop()

	expiresAt := time.Now().Add(l.opts.ttl)
	for {
		select {
		case <-l.stopRenewCh:
			return
		case <-ticker.C:
		}

		if time.Now().After(expiresAt) {
			l.notifyLost(ErrLockLost)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), l.opts.commandTimeout())
		err := l.Refresh(ctx)
		cancel()
		if err == nil {
			expiresAt = time.Now().Add(l.opts.ttl)
			continue
		}
		if errors.Is(err, ErrLockReleased) {
			return
		}
		if errors.Is(err, ErrLockLost) {
			l.notifyLost(err)
			return
		}
		if time.Now().Add(l.opts.renewInterval).After(expiresAt) {
			l.notifyLost(fmt.Errorf("%w: renew failed before ttl expired: %v", ErrLockLost, err))
			return
		}
	}
}

func (l *Lock) stopRenew() {
	l.stopOnce.Do(func() {
		close(l.stopRenewCh)
		<-l.renewDoneCh
	})
}

func (l *Lock) notifyLost(err error) {
	if err == nil {
		err = ErrLockLost
	}
	l.lostOnce.Do(func() {
		l.lostCh <- err
	})
}

func applyOptions(base options, opts ...Option) (options, error) {
	cp := base
	for _, opt := range opts {
		if opt != nil {
			opt(&cp)
		}
	}
	if cp.ttl <= 0 {
		return cp, fmt.Errorf("%w: ttl must be positive", ErrInvalidOption)
	}
	if cp.spinInterval <= 0 {
		return cp, fmt.Errorf("%w: spin interval must be positive", ErrInvalidOption)
	}
	if !cp.renewIntervalSet {
		cp.renewInterval = cp.ttl / 3
	}
	if cp.renewInterval <= 0 {
		return cp, fmt.Errorf("%w: renew interval must be positive", ErrInvalidOption)
	}
	if cp.autoRenew && cp.renewInterval >= cp.ttl {
		return cp, fmt.Errorf("%w: renew interval must be less than ttl", ErrInvalidOption)
	}
	return cp, nil
}

func withOptions(opts options) Option {
	return func(o *options) {
		*o = opts
	}
}

func (o options) commandTimeout() time.Duration {
	timeout := o.renewInterval
	if timeout <= 0 || timeout > o.ttl/2 {
		timeout = o.ttl / 2
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	if timeout > 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

func durationMillis(d time.Duration) int64 {
	ms := d.Milliseconds()
	if ms <= 0 {
		return 1
	}
	return ms
}

var acquireScript = redis.NewScript(`
local value = redis.call("GET", KEYS[1])
if not value then
	redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
	return 1
end
if value == ARGV[1] then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end
return 0
`)

var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

var refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)
