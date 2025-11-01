package stringslicepool

import (
	"math"
	"math/bits"
	"sync"
	"unsafe"
)

var builtinPool Pool

// Pool consists of 32 sync.Pool, representing byte slices of length from 0 to 32 in powers of 2.
type Pool struct {
	pools [32]sync.Pool
}

// Get returns a byte slice with given length from the built-in pool.
func Get(size int) []string {
	return builtinPool.Get(size)
}

// Put returns the byte slice to the built-in pool.
func Put(buf []string) {
	builtinPool.Put(buf)
}

// Get retrieves a byte slice of the length requested by the caller from pool or allocates a new one.
func (p *Pool) Get(size int) []string {
	if size <= 0 {
		return nil
	}
	if size > math.MaxInt32 {
		return make([]string, size)
	}
	idx := index(uint32(size))
	ptr, _ := p.pools[idx].Get().(*string)
	if ptr == nil {
		return make([]string, size, 1<<idx)
	}
	return unsafe.Slice(ptr, 1<<idx)[:size]
}

// Put returns the byte slice to the pool.
func (p *Pool) Put(buf []string) {
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
