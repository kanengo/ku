package segment

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kanengo/ku/distributedx"
)

// TestSegmentIDGen_Integration runs an integration test against a PostgreSQL database.
// It requires a valid DSN to be set in the TEST_PG_DSN environment variable.
// Default DSN is provided for local testing.
func TestSegmentIDGen_Integration(t *testing.T) {
	// User can adjust this default DSN or set the environment variable
	defaultDSN := "postgres://soular:CDdyJmCQ0r0YdBYZ6EWB@43.133.32.63:6432/soular?sslmode=disable"
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := "id_generator_test"
	schema := "infra" // Matches the search_path in DSN

	// Clean up table before test to ensure clean state
	// We need a raw connection for this
	conn, err := distributedx.GetConn(dsn)
	if err == nil {
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", schema, tableName))
	}

	// 1. Initialize Generator
	config := Config{
		DSN:       dsn,
		TableName: tableName,
		Schema:    schema,
	}

	gen, err := NewSegmentIDGen(ctx, config)
	if err != nil {
		t.Logf("Failed to connect to database with DSN: %s", dsn)
		t.Logf("Error: %v", err)
		t.Skip("Skipping test due to connection failure")
		return
	}
	defer gen.Close()

	// 2. Init Business Tag
	bizTag := int32(1001)
	step := int32(10) // Small step for testing buffer switching
	startId := int64(0)

	err = gen.Init(ctx, bizTag, startId, step)
	require.NoError(t, err, "Failed to init biz tag")

	// 3. Test Basic Generation
	t.Run("BasicGeneration", func(t *testing.T) {
		id1, err := gen.GetID(ctx, bizTag)
		require.NoError(t, err)
		t.Logf("Generated ID 1: %d", id1)
		// First ID should be startId + 1 = 1
		assert.Equal(t, int64(1), id1)

		id2, err := gen.GetID(ctx, bizTag)
		require.NoError(t, err)
		t.Logf("Generated ID 2: %d", id2)
		assert.Equal(t, id1+1, id2, "IDs should be sequential")
	})

	// 4. Test Concurrency
	t.Run("ConcurrentGeneration", func(t *testing.T) {
		var wg sync.WaitGroup
		count := 50
		ids := make(chan int64, count)
		errs := make(chan error, count)

		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, err := gen.GetID(ctx, bizTag)
				if err != nil {
					errs <- err
				} else {
					ids <- id
				}
			}()
		}

		wg.Wait()
		close(ids)
		close(errs)

		require.Empty(t, errs, "Should not have errors in concurrent generation")

		collectedIDs := make(map[int64]bool)
		for id := range ids {
			assert.False(t, collectedIDs[id], "Duplicate ID generated: %d", id)
			collectedIDs[id] = true
		}
		assert.Equal(t, count, len(collectedIDs), "Should generate unique IDs for all requests")
	})

	// 5. Test Restart/Recovery
	t.Run("RestartRecovery", func(t *testing.T) {
		// Create a new generator instance to simulate service restart
		gen2, err := NewSegmentIDGen(ctx, config)
		require.NoError(t, err)
		defer gen2.Close()

		// Init again (should reuse existing record)
		err = gen2.Init(ctx, bizTag, startId, step)
		require.NoError(t, err)

		id, err := gen2.GetID(ctx, bizTag)
		require.NoError(t, err)
		t.Logf("ID after restart: %d", id)

		// The new ID should be greater than any previously generated ID
		// Previous usage: 2 (Basic) + 50 (Concurrent) = 52.
		// Segments consumed:
		// 1-10 (used 2) -> Trigger async load -> Next 11-20
		// ...
		// We used 52 IDs.
		// Last ID used was 52.
		// Next ID should be 53?
		// But since we allocate in blocks of 10, and we restart, we might lose the unused IDs in the current buffer.
		// If current buffer was 51-60, and we used 51, 52.
		// Max ID in DB is 60 (or 70 if async loaded).
		// So next fetch will get 61-70 (or 71-80).
		// So ID should be > 52.
		assert.Greater(t, id, int64(52))
	})
}
