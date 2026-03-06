package snowflake

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnowflake_Integration runs an integration test against a PostgreSQL database.
// It requires a valid DSN to be set in the TEST_PG_DSN environment variable
// or you can modify the defaultDSN constant below.
// The DSN should include the schema in the search_path if needed, e.g.:
// "postgres://user:password@localhost:5432/dbname?search_path=myschema"
func TestSnowflake_Integration(t *testing.T) {
	// User can adjust this default DSN or set the environment variable
	defaultDSN := "postgres://soular:CDdyJmCQ0r0YdBYZ6EWB@43.133.32.63:6432/soular?sslmode=disable"
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = defaultDSN
		// Uncomment the following line if you want to skip the test when env var is not set
		// t.Skip("Skipping integration test: TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := "snowflake_workers_test"

	// Configuration for the snowflake worker
	config := Config{
		DSN:       dsn,
		TableName: tableName,
		Epoch:     1704067200000, // 2024-01-01 00:00:00 UTC
	}

	// 1. Create the first snowflake instance
	sf1, err := New(ctx, config)
	if err != nil {
		t.Logf("Failed to connect to database with DSN: %s", dsn)
		t.Logf("Error: %v", err)
		t.Skip("Skipping test due to connection failure (check your DSN)")
		return
	}
	defer sf1.Close()

	t.Logf("Successfully connected. Worker ID: %d", sf1.GetWorkerID())

	// 2. Generate some IDs
	id1 := sf1.Generate()
	t.Logf("Generated ID 1: %d", id1)
	assert.Greater(t, id1, int64(0))

	id2 := sf1.Generate()
	t.Logf("Generated ID 2: %d", id2)
	assert.Greater(t, id2, id1, "IDs should be increasing")

	// 3. Create a second snowflake instance to test worker ID allocation
	// Note: We use the same config, so it should try to allocate a new worker ID
	sf2, err := New(ctx, config)
	require.NoError(t, err)
	defer sf2.Close()

	t.Logf("Second worker connected. Worker ID: %d", sf2.GetWorkerID())
	assert.NotEqual(t, sf1.GetWorkerID(), sf2.GetWorkerID(), "Worker IDs should be different")

	id3 := sf2.Generate()
	t.Logf("Generated ID 3 (from worker 2): %d", id3)
	assert.Greater(t, id3, int64(0))

	// 4. Clean up (Optional: drop the test table)
	// We generally don't drop tables in integration tests unless we want a clean slate every time,
	// but here we might want to clean up to avoid polluting the DB.
	// However, the user might want to inspect the table. I'll leave it.
}
