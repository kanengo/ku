package mapx

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardMapBasicOperations(t *testing.T) {
	m := NewShardMap[int, string](ShardMapOptions[int, string]{ShardNum: 4})

	m.Set(1, "one")
	m.Set(2, "two")
	m.Set(1, "ONE")

	v, ok := m.Get(1)
	require.True(t, ok)
	assert.Equal(t, "ONE", v)
	assert.True(t, m.Has(2))
	assert.Equal(t, 2, m.Len())

	m.Delete(2)
	_, ok = m.Get(2)
	assert.False(t, ok)
	assert.Equal(t, 1, m.Len())

	m.Clear()
	assert.Equal(t, 0, m.Len())
}

func TestShardMapRange(t *testing.T) {
	m := NewShardMap[string, int](ShardMapOptions[string, int]{ShardNum: 2})
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	got := make(map[string]int)
	m.Range(func(k string, v int) bool {
		got[k] = v
		return true
	})

	assert.Equal(t, map[string]int{"a": 1, "b": 2, "c": 3}, got)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, m.Keys())
	assert.ElementsMatch(t, []int{1, 2, 3}, m.Values())
}

func TestShardMapConcurrentAccess(t *testing.T) {
	m := NewShardMap[int, string](ShardMapOptions[int, string]{ShardNum: 8})

	const total = 1000
	var wg sync.WaitGroup
	for i := range total {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i, fmt.Sprintf("value-%d", i))
			v, ok := m.Get(i)
			require.True(t, ok)
			require.Equal(t, fmt.Sprintf("value-%d", i), v)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, total, m.Len())
}
