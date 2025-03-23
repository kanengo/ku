package goroutinex

import (
	"sync/atomic"

	"github.com/kanengo/ku/synchronize/queuex"
)

// Pool 有上限的goroutine池,当达到上限时,会直接原地执行,并不会创建新的goroutine
// 当缓冲队列数小于当前worker数时,会将任务放入缓冲队列,否则会new goroutine执行
type Pool struct {
	maxWorkers      int64
	bufferQueue     *queuex.RingQueue[func()]
	idleWorkerQueue *queuex.RingQueue[*worker]

	maxIdleWokers int64

	idle    atomic.Int64
	running atomic.Int64
}

type Config struct {
	MaxWorkers      int64
	BufferQueueSize int64
	MaxIdleWokers   int64
}

const (
	defaultMaxWorkers      = 1 << 17
	defaultMaxIdleWokers   = 1 << 10
	defaultBufferQueueSize = 1 << 10
)

//go:norace
func NewPool(config Config) *Pool {
	if config.BufferQueueSize < 1 {
		config.BufferQueueSize = defaultBufferQueueSize
	}
	if config.MaxIdleWokers < 1 {
		config.MaxIdleWokers = defaultMaxIdleWokers
	}
	if config.MaxWorkers < 1 {
		config.MaxWorkers = defaultMaxWorkers
	}

	p := &Pool{
		maxWorkers:      config.MaxWorkers,
		bufferQueue:     queuex.NewRingQueue[func()](config.BufferQueueSize),
		idleWorkerQueue: queuex.NewRingQueue[*worker](config.MaxIdleWokers),
		maxIdleWokers:   config.MaxIdleWokers,
	}

	return p
}

//go:norace
func (p *Pool) fork(task func()) {
	p.running.Add(1)
	t := task
	go func() {
		defer func() {
			p.running.Add(-1)
		}()
		for {
			t()
			var ok bool
			t, ok = p.bufferQueue.Dequeue()
			if ok {
				continue
			}
			break
		}
	}()
}

//go:norace
func (p *Pool) insertWorker(w *worker) {
	p.idleWorkerQueue.Enqueue(w)
}

//go:norace
func (p *Pool) newWorker() *worker {
	w := newWorker()
	w.p = p
	p.idle.Add(1)
	return w
}

//go:norace
func (p *Pool) Go(fn func()) {
	if fn == nil {
		return
	}

	running := p.running.Load()
	if running >= p.maxWorkers { // 已达到上限,直接执行
		fn()
		return
	}

	w, _ := p.idleWorkerQueue.Dequeue()
	if w != nil {
		w.addTask(fn)
		return
	}

	idle := p.idle.Load()
	if idle < p.maxIdleWokers {
		w := p.newWorker()
		w.addTask(fn)
		return
	}

	bufferLen := p.bufferQueue.Length()
	if bufferLen <= int(running)*2/3 && bufferLen < p.bufferQueue.Capacity() {
		if p.bufferQueue.Enqueue(fn) {
			return
		}
	}

	// worker数未达到上限,直接创建
	p.fork(fn)
}

//go:norace
func (p *Pool) Close() {
	//TODO
}
