package signalx

import "sync/atomic"

type Signal struct {
	sig atomic.Value
}

func NewSignal() *Signal {
	sig := atomic.Value{}
	sig.Store(make(chan struct{}))
	return &Signal{
		sig: sig,
	}
}

func (s *Signal) Signal() {
	newCh := make(chan struct{})
	oldCh := s.sig.Swap(newCh).(chan struct{})
	close(oldCh)
}

func (s *Signal) Wait() {
	c := s.sig.Load().(chan struct{})
	<-c
}
