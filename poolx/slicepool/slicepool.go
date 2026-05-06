package slicepool

import (
	"math"
	"math/bits"
	"sync"
	"unsafe"
)

// Pool consists of 32 sync.Pool, representing byte slices of length from 0 to 32 in powers of 2.
type Pool[T any] struct {
	pools [32]sync.Pool
}

// Get retrieves a byte slice of the length requested by the caller from pool or allocates a new one.
func (p *Pool[T]) Get(size int) []T {
	if size <= 0 {
		return nil
	}
	if size > math.MaxInt32 {
		return make([]T, size)
	}
	idx := index(uint32(size))
	ptr, _ := p.pools[idx].Get().(*T)
	if ptr == nil {
		return make([]T, size, 1<<idx)
	}
	return unsafe.Slice(ptr, 1<<idx)[:size]
}

// Put returns the byte slice to the pool.
func (p *Pool[T]) Put(buf []T) {
	size := cap(buf)
	if size == 0 || size > math.MaxInt32 {
		return
	}
	idx := index(uint32(size))
	if size != 1<<idx { // this byte slice is not from Pool.Get(), put it into the previous interval of idx
		idx--
	}
	// Store the pointer to the underlying array instead of the pointer to the slice itself,
	// which circumvents the escape of buf from the stack to the heap.
	p.pools[idx].Put(unsafe.SliceData(buf))
}

func index(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}
