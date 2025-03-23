package goroutinex

import (
	"sync"
)

type worker struct {
	taskCh chan func()
	stopCh chan struct{}

	p *Pool
}

var wp = &sync.Pool{
	New: func() any {
		return newWorker()
	},
}

//go:norace
func newWorker() *worker {
	w := &worker{
		taskCh: make(chan func(), 1),
		stopCh: make(chan struct{}),
	}
	go w.run()
	return w
}

//go:norace
func (w *worker) exec(fn func()) {
	w.p.running.Add(1)
	defer func() {
		w.p.running.Add(-1)
		w.p.insertWorker(w)
	}()
	t := fn
	for {
		t()
		t, _ = w.p.bufferQueue.Dequeue()
		if t != nil {
			continue
		}
		break
	}
}

//go:norace
func (w *worker) run() {
	for {
		select {
		case fn := <-w.taskCh:
			w.exec(fn)
		case <-w.stopCh:
			return
		}
	}
}

//go:norace
func (w *worker) addTask(fn func()) {
	w.taskCh <- fn
}

//go:norace
func (w *worker) stop() {
	w.stopCh <- struct{}{}
}
