package queuex

import (
	"sync/atomic"

	"github.com/kanengo/ku/mathx"
)

type RingQueue[T any] struct {
	buffer []*slot[T]
	size   int
	mask   int

	head uint64
	_    [56]byte
	tail uint64
}

type slot[T any] struct {
	item  T
	valid atomic.Bool
}

//go:norace
func NewRingQueue[T any, S ~int | ~int64 | ~uint | ~uint64](size S) *RingQueue[T] {
	size = mathx.NextPowerOfTwo(size)
	buffer := make([]*slot[T], size)
	for i := range buffer {
		buffer[i] = &slot[T]{}
	}

	q := &RingQueue[T]{
		buffer: buffer,
		size:   int(size),
		mask:   int(size) - 1,
	}

	return q
}

//go:norace
func (q *RingQueue[T]) Enqueue(item T) bool {
	var tail, head, pos uint64
	var slot *slot[T]
	for {
		tail = atomic.LoadUint64(&q.tail)
		pos = tail & uint64(q.mask)
		slot = q.buffer[pos]

		if slot.valid.Load() {
			head = atomic.LoadUint64(&q.head)
			if tail-head >= uint64(q.size) {
				return false
			}
			continue
		}

		if atomic.CompareAndSwapUint64(&q.tail, tail, tail+1) {
			slot.item = item
			slot.valid.Store(true)
			return true
		}
	}
}

//go:norace
func (q *RingQueue[T]) Dequeue() (T, bool) {
	var head, tail, pos uint64
	var slot *slot[T]
	var item T
	for {
		head = atomic.LoadUint64(&q.head)
		tail = atomic.LoadUint64(&q.tail)
		if head >= tail {
			return item, false
		}
		pos = head & uint64(q.mask)
		slot = q.buffer[pos]

		if !slot.valid.Load() {
			continue
		}

		if atomic.CompareAndSwapUint64(&q.head, head, head+1) {
			item = slot.item
			slot.valid.Store(false)
			return item, true
		}
	}
}

//go:norace
func (q *RingQueue[T]) Length() int {
	head := atomic.LoadUint64(&q.head)
	tail := atomic.LoadUint64(&q.tail)
	return int(tail - head)
}

//go:norace
func (q *RingQueue[T]) Full() bool {
	return q.Length() == q.size
}

//go:norace
func (q *RingQueue[T]) Capacity() int {
	return q.size
}
