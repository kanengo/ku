package snowflakex

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewNode(t *testing.T) {
	node, err := NewNode(1, 0)
	assert.NoError(t, err)
	assert.NotNil(t, node)

	node, err = NewNode(MaxWorkerID+1, 0)
	assert.Error(t, err)
	assert.Nil(t, node)

	node, err = NewNode(-1, 0)
	assert.Error(t, err)
	assert.Nil(t, node)
}

func TestGenerate(t *testing.T) {
	node, err := NewNode(1, 0)
	assert.NoError(t, err)

	id := node.Generate()
	assert.Greater(t, id, int64(0))
}

func TestUniqueness(t *testing.T) {
	node, err := NewNode(1, 0)
	assert.NoError(t, err)

	count := 1000
	ids := make(map[int64]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := node.Generate()
			mu.Lock()
			ids[id] = true
			mu.Unlock()
		}()
	}

	wg.Wait()
	assert.Equal(t, count, len(ids))
}

func TestMonotonicity(t *testing.T) {
	node, err := NewNode(1, 0)
	assert.NoError(t, err)

	lastID := int64(0)
	for i := 0; i < 1000; i++ {
		id := node.Generate()
		assert.Greater(t, id, lastID)
		lastID = id
	}
}

func TestWorkerID(t *testing.T) {
	workerID := int64(10)
	node, err := NewNode(workerID, 0)
	assert.NoError(t, err)

	id := node.Generate()
	// Extract worker ID
	extractedWorkerID := (id >> WorkerIDShift) & MaxWorkerID
	assert.Equal(t, workerID, extractedWorkerID)
}

func TestEpoch(t *testing.T) {
	// Set epoch to 1 hour ago
	epoch := time.Now().Add(-1 * time.Hour).UnixMilli()
	node, err := NewNode(1, epoch)
	assert.NoError(t, err)

	id := node.Generate()
	// Extract timestamp
	timestamp := (id >> TimestampShift) + epoch
	now := time.Now().UnixMilli()

	// Allow small difference due to execution time
	assert.InDelta(t, now, timestamp, 100)
}

func TestGetTimeFromID(t *testing.T) {
	// Use default epoch
	node, err := NewNode(1, 0)
	assert.NoError(t, err)

	id := node.Generate()
	timestamp := node.GetTimeFromID(id)
	now := time.Now().UnixMilli()

	assert.InDelta(t, now, timestamp, 100)

	// Use custom epoch
	customEpoch := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	node2, err := NewNode(1, customEpoch)
	assert.NoError(t, err)

	id2 := node2.Generate()
	timestamp2 := node2.GetTimeFromID(id2)
	now2 := time.Now().UnixMilli()

	assert.InDelta(t, now2, timestamp2, 100)
}

func TestGetTime(t *testing.T) {
	node, err := NewNode(1, 0)
	assert.NoError(t, err)

	id := node.Generate()
	tm := node.GetTime(id)
	now := time.Now()

	// Compare UnixMilli values
	assert.InDelta(t, now.UnixMilli(), tm.UnixMilli(), 100)
}
