package generalconfig

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGet_NotInitialized(t *testing.T) {
	old := Default()
	SetDefault(nil)
	t.Cleanup(func() {
		SetDefault(old)
	})

	_, err := Get[int](context.Background(), "missing")
	require.ErrorIs(t, err, ErrNotInitialized)

	got, err := Get[int](context.Background(), "missing", WithDefaultValue(42))
	require.NoError(t, err)
	assert.Equal(t, 42, got)
}

func TestGetFrom_NilManager(t *testing.T) {
	_, err := GetFrom[string](context.Background(), nil, "key")
	require.ErrorIs(t, err, ErrNotInitialized)

	got, err := GetFrom[string](context.Background(), nil, "key", WithDefaultValue("fallback"))
	require.NoError(t, err)
	assert.Equal(t, "fallback", got)

	got, err = GetFrom[string](context.Background(), &Manager{}, "key", WithDefaultValue("fallback"))
	require.NoError(t, err)
	assert.Equal(t, "fallback", got)
}

func TestDefaultManagerAccessors(t *testing.T) {
	old := Default()
	t.Cleanup(func() {
		SetDefault(old)
	})

	manager := &Manager{}
	SetDefault(manager)
	assert.Same(t, manager, Default())

	SetDefault(nil)
	assert.Nil(t, Default())
}

func TestApplyOptions(t *testing.T) {
	opts, err := applyOptions(defaultOptions,
		WithSchema("custom_schema"),
		WithTable("custom_table"),
		WithDefaultRefreshInterval(2*time.Minute),
		WithQueryTimeout(time.Second),
	)
	require.NoError(t, err)
	assert.Equal(t, "custom_schema", opts.schema)
	assert.Equal(t, "custom_table", opts.table)
	assert.Equal(t, 2*time.Minute, opts.defaultRefreshInterval)
	assert.Equal(t, time.Second, opts.queryTimeout)

	_, err = applyOptions(defaultOptions, WithSchema("bad-schema"))
	require.ErrorIs(t, err, ErrInvalidOption)

	_, err = applyOptions(defaultOptions, WithTable("1bad"))
	require.ErrorIs(t, err, ErrInvalidOption)
}

func TestApplyWriteOptions(t *testing.T) {
	opts, err := applyWriteOptions(WithStatus(StatusDisabled), WithRefreshInterval(10))
	require.NoError(t, err)
	assert.True(t, opts.hasStatus)
	assert.Equal(t, StatusDisabled, opts.status)
	assert.True(t, opts.hasRefresh)
	assert.Equal(t, 10, opts.refreshInterval)

	_, err = applyWriteOptions(WithStatus(9))
	require.ErrorIs(t, err, ErrInvalidOption)

	_, err = applyWriteOptions(WithRefreshInterval(-1))
	require.ErrorIs(t, err, ErrInvalidOption)
}

func TestDecodeRaw(t *testing.T) {
	type config struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}

	got, err := decodeRaw[config](`{"name":"demo","enabled":true}`)
	require.NoError(t, err)
	assert.Equal(t, "demo", got.Name)
	assert.True(t, got.Enabled)

	_, err = decodeRaw[config](`{"name":`)
	require.Error(t, err)
}

func TestEntryUpdateRefreshInterval(t *testing.T) {
	e := newEntry("k", time.Minute)
	e.update(ConfigRecord{Value: `{"ok":true}`, RefreshInterval: 2}, time.Minute)
	assert.Equal(t, `{"ok":true}`, e.value())
	assert.Equal(t, 2*time.Second, e.interval())

	e.update(ConfigRecord{Value: `{"ok":false}`, RefreshInterval: 0}, time.Minute)
	assert.Equal(t, time.Minute, e.interval())
}

func TestEntryTypedCache(t *testing.T) {
	type config struct {
		Name string `json:"name"`
	}

	e := newEntry("k", time.Minute)
	e.update(ConfigRecord{Value: `{"name":"first"}`, RefreshInterval: 1}, time.Minute)

	got, err := entryValueAs[config](e)
	require.NoError(t, err)
	assert.Equal(t, "first", got.Name)

	typ := typeOf[config]()
	e.mu.RLock()
	cached, ok := e.typed[typ]
	version := e.version
	e.mu.RUnlock()
	require.True(t, ok)
	assert.Equal(t, version, cached.version)

	got, err = entryValueAs[config](e)
	require.NoError(t, err)
	assert.Equal(t, "first", got.Name)

	e.update(ConfigRecord{Value: `{"name":"second"}`, RefreshInterval: 1}, time.Minute)
	got, err = entryValueAs[config](e)
	require.NoError(t, err)
	assert.Equal(t, "second", got.Name)

	e.mu.RLock()
	assert.Greater(t, e.version, version)
	assert.Len(t, e.typed, 1)
	e.mu.RUnlock()
}

func TestEntryTypedCacheNullAny(t *testing.T) {
	e := newEntry("k", time.Minute)
	e.update(ConfigRecord{Value: `null`, RefreshInterval: 1}, time.Minute)

	got, err := entryValueAs[any](e)
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = entryValueAs[any](e)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMapNoRows(t *testing.T) {
	err := mapNoRows(ErrEmptyKey)
	require.ErrorIs(t, err, ErrEmptyKey)
	assert.False(t, errors.Is(err, ErrNotFound))
}
