package generalconfig

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if dsn == "" {
		t.Skip("Skipping integration test: TEST_PG_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	schema := "infra"
	table := fmt.Sprintf("general_configs_test_%d", time.Now().UnixNano())

	manager, err := NewManager(ctx, dsn,
		WithSchema(schema),
		WithTable(table),
		WithDefaultRefreshInterval(time.Second),
		WithQueryTimeout(5*time.Second),
	)
	if err != nil {
		t.Skipf("Skipping integration test because postgres is unavailable: %v", err)
	}
	defer manager.Close()
	t.Cleanup(func() {
		_, _ = manager.pg.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, schema, table))
	})

	type sampleConfig struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}

	record, err := manager.Create(ctx, "feature.flag", sampleConfig{Name: "first", Enabled: true}, WithRefreshInterval(1))
	require.NoError(t, err)
	assert.Equal(t, StatusEnabled, record.Status)
	assert.Equal(t, 1, record.RefreshInterval)

	got, err := GetFrom[sampleConfig](ctx, manager, "feature.flag")
	require.NoError(t, err)
	assert.Equal(t, "first", got.Name)
	assert.True(t, got.Enabled)

	_, err = manager.Update(ctx, "feature.flag", sampleConfig{Name: "second", Enabled: false})
	require.NoError(t, err)

	got, err = GetFrom[sampleConfig](ctx, manager, "feature.flag", WithForceRefresh[sampleConfig]())
	require.NoError(t, err)
	assert.Equal(t, "second", got.Name)
	assert.False(t, got.Enabled)

	records, err := manager.List(ctx)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	require.NoError(t, manager.Delete(ctx, "feature.flag"))
	got, err = GetFrom[sampleConfig](ctx, manager, "feature.flag", WithDefaultValue(sampleConfig{Name: "fallback"}))
	require.NoError(t, err)
	assert.Equal(t, "fallback", got.Name)
}
