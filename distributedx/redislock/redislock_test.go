package redislock

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryLockStoresTTLAndUnlocks(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(time.Second), WithAutoRenew(false))
	require.NoError(t, err)

	lock, err := locker.TryLock(ctx, "locks:a")
	require.NoError(t, err)
	assert.Equal(t, "locks:a", lock.Key())
	assert.NotEmpty(t, lock.ID())

	value, ok := rdb.value("locks:a")
	require.True(t, ok)
	assert.Equal(t, lock.ID(), value)

	ttl, ok := rdb.ttl("locks:a")
	require.True(t, ok)
	assert.Equal(t, time.Second, ttl)

	require.NoError(t, lock.Unlock(ctx))
	_, ok = rdb.value("locks:a")
	assert.False(t, ok)
}

func TestTryLockRejectsDifferentID(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(time.Second), WithAutoRenew(false))
	require.NoError(t, err)

	lock, err := locker.TryLockWithID(ctx, "locks:a", "id-a")
	require.NoError(t, err)
	defer func() { _ = lock.Unlock(ctx) }()

	_, err = locker.TryLockWithID(ctx, "locks:a", "id-b")
	assert.ErrorIs(t, err, ErrLockNotAcquired)

	value, ok := rdb.value("locks:a")
	require.True(t, ok)
	assert.Equal(t, "id-a", value)
}

func TestLockWaitsAndRespectsContext(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(time.Second), WithSpinInterval(10*time.Millisecond), WithAutoRenew(false))
	require.NoError(t, err)

	lock, err := locker.TryLockWithID(ctx, "locks:a", "id-a")
	require.NoError(t, err)
	defer func() { _ = lock.Unlock(ctx) }()

	waitCtx, cancel := context.WithTimeout(ctx, 35*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = locker.LockWithID(waitCtx, "locks:a", "id-b")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
}

func TestSameIDRefreshesTTL(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(100*time.Millisecond), WithAutoRenew(false))
	require.NoError(t, err)

	lock, err := locker.TryLockWithID(ctx, "locks:a", "id-a")
	require.NoError(t, err)

	rdb.advance(80 * time.Millisecond)

	refreshed, err := locker.TryLockWithID(ctx, "locks:a", "id-a")
	require.NoError(t, err)

	rdb.advance(80 * time.Millisecond)
	value, ok := rdb.value("locks:a")
	require.True(t, ok)
	assert.Equal(t, "id-a", value)

	rdb.advance(30 * time.Millisecond)
	_, ok = rdb.value("locks:a")
	assert.False(t, ok)

	assert.ErrorIs(t, lock.Unlock(ctx), ErrLockNotOwned)
	assert.ErrorIs(t, refreshed.Unlock(ctx), ErrLockNotOwned)
}

func TestUnlockRequiresSameID(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(time.Second), WithAutoRenew(false))
	require.NoError(t, err)

	lock, err := locker.TryLockWithID(ctx, "locks:a", "id-a")
	require.NoError(t, err)
	defer func() { _ = lock.Unlock(ctx) }()

	wrong := newTestLock(locker, lock.opts, "locks:a", "id-b")
	err = wrong.Unlock(ctx)
	assert.ErrorIs(t, err, ErrLockNotOwned)

	value, ok := rdb.value("locks:a")
	require.True(t, ok)
	assert.Equal(t, "id-a", value)
}

func TestRefreshRequiresSameIDAndReportsLost(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(time.Second), WithAutoRenew(false))
	require.NoError(t, err)

	lock, err := locker.TryLockWithID(ctx, "locks:a", "id-a")
	require.NoError(t, err)

	rdb.set("locks:a", "id-b", time.Second)

	err = lock.Refresh(ctx)
	assert.ErrorIs(t, err, ErrLockLost)
	assert.ErrorIs(t, lock.Unlock(ctx), ErrLockNotOwned)
}

func TestAutoRenewStartsAndStops(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(100*time.Millisecond), WithRenewInterval(10*time.Millisecond))
	require.NoError(t, err)

	lock, err := locker.TryLock(ctx, "locks:a")
	require.NoError(t, err)
	require.True(t, waitForCondition(200*time.Millisecond, func() bool {
		return rdb.calls.Load() >= 2
	}))

	require.NoError(t, lock.Unlock(ctx))
	afterUnlock := rdb.calls.Load()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, afterUnlock, rdb.calls.Load())
}

func TestRunCancelsContextWhenLockLost(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	locker, err := New(rdb, WithTTL(100*time.Millisecond), WithRenewInterval(10*time.Millisecond))
	require.NoError(t, err)

	err = locker.RunWithID(ctx, "locks:a", "id-a", func(runCtx context.Context) error {
		rdb.set("locks:a", "id-b", time.Second)
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-time.After(time.Second):
			t.Fatal("run context was not canceled after lock loss")
			return nil
		}
	})
	assert.ErrorIs(t, err, ErrLockLost)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestValidation(t *testing.T) {
	ctx := context.Background()
	rdb := newFakeRedis()

	_, err := New(nil)
	assert.ErrorIs(t, err, ErrNilRedisClient)

	locker, err := New(rdb)
	require.NoError(t, err)

	_, err = locker.TryLock(ctx, "")
	assert.ErrorIs(t, err, ErrEmptyKey)

	_, err = locker.TryLockWithID(ctx, "locks:a", "")
	assert.ErrorIs(t, err, ErrEmptyLockID)

	_, err = New(rdb, WithTTL(0))
	assert.ErrorIs(t, err, ErrInvalidOption)

	_, err = New(rdb, WithTTL(time.Second), WithRenewInterval(time.Second))
	assert.ErrorIs(t, err, ErrInvalidOption)

	assert.NotEmpty(t, NewID())
}

type fakeRedis struct {
	redis.Cmdable
	mu     sync.Mutex
	now    time.Time
	values map[string]fakeRedisValue
	calls  atomic.Int64
}

type fakeRedisValue struct {
	value     string
	expiresAt time.Time
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{
		now:    time.Unix(0, 0),
		values: make(map[string]fakeRedisValue),
	}
}

func (f *fakeRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if err := ctx.Err(); err != nil {
		return redis.NewCmdResult(nil, err)
	}

	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(keys) != 1 {
		return redis.NewCmdResult(nil, ErrEmptyKey)
	}
	switch sha1 {
	case acquireScript.Hash():
		return redis.NewCmdResult(f.acquire(keys[0], stringArg(args, 0), durationArg(args, 1)), nil)
	case unlockScript.Hash():
		return redis.NewCmdResult(f.unlock(keys[0], stringArg(args, 0)), nil)
	case refreshScript.Hash():
		return redis.NewCmdResult(f.refresh(keys[0], stringArg(args, 0), durationArg(args, 1)), nil)
	default:
		return redis.NewCmdResult(nil, redis.Nil)
	}
}

func (f *fakeRedis) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return redis.NewCmdResult(nil, redis.Nil)
}

func (f *fakeRedis) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return redis.NewCmdResult(nil, redis.Nil)
}

func (f *fakeRedis) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return redis.NewCmdResult(nil, redis.Nil)
}

func (f *fakeRedis) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	ret := make([]bool, len(hashes))
	for i := range ret {
		ret[i] = true
	}
	return redis.NewBoolSliceResult(ret, nil)
}

func (f *fakeRedis) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return redis.NewStringResult("", nil)
}

func (f *fakeRedis) acquire(key, id string, ttl time.Duration) int64 {
	v, ok := f.getLocked(key)
	if !ok {
		f.values[key] = fakeRedisValue{
			value:     id,
			expiresAt: f.now.Add(ttl),
		}
		return 1
	}
	if v.value != id {
		return 0
	}
	v.expiresAt = f.now.Add(ttl)
	f.values[key] = v
	return 1
}

func (f *fakeRedis) unlock(key, id string) int64 {
	v, ok := f.getLocked(key)
	if !ok || v.value != id {
		return 0
	}
	delete(f.values, key)
	return 1
}

func (f *fakeRedis) refresh(key, id string, ttl time.Duration) int64 {
	v, ok := f.getLocked(key)
	if !ok || v.value != id {
		return 0
	}
	v.expiresAt = f.now.Add(ttl)
	f.values[key] = v
	return 1
}

func (f *fakeRedis) set(key, value string, ttl time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.values[key] = fakeRedisValue{
		value:     value,
		expiresAt: f.now.Add(ttl),
	}
}

func (f *fakeRedis) value(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.getLocked(key)
	if !ok {
		return "", false
	}
	return v.value, true
}

func (f *fakeRedis) ttl(key string) (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.getLocked(key)
	if !ok {
		return 0, false
	}
	return v.expiresAt.Sub(f.now), true
}

func (f *fakeRedis) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = f.now.Add(d)
	for key := range f.values {
		f.getLocked(key)
	}
}

func (f *fakeRedis) getLocked(key string) (fakeRedisValue, bool) {
	v, ok := f.values[key]
	if !ok {
		return fakeRedisValue{}, false
	}
	if !v.expiresAt.IsZero() && !f.now.Before(v.expiresAt) {
		delete(f.values, key)
		return fakeRedisValue{}, false
	}
	return v, true
}

func stringArg(args []interface{}, idx int) string {
	if idx >= len(args) {
		return ""
	}
	v, _ := args[idx].(string)
	return v
}

func durationArg(args []interface{}, idx int) time.Duration {
	if idx >= len(args) {
		return 0
	}
	switch v := args[idx].(type) {
	case int64:
		return time.Duration(v) * time.Millisecond
	case int:
		return time.Duration(v) * time.Millisecond
	case int32:
		return time.Duration(v) * time.Millisecond
	case string:
		d, _ := time.ParseDuration(v + "ms")
		return d
	default:
		return 0
	}
}

func newTestLock(locker *Locker, options options, key, id string) *Lock {
	lock := &Lock{
		locker:      locker,
		opts:        options,
		key:         key,
		id:          id,
		stopRenewCh: make(chan struct{}),
		renewDoneCh: make(chan struct{}),
		lostCh:      make(chan error, 1),
	}
	close(lock.renewDoneCh)
	return lock
}

func waitForCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return fn()
}

var _ redis.Cmdable = (*fakeRedis)(nil)
