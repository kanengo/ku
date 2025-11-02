package goroutinex

import "github.com/kanengo/ku/basex"

func GoSafe(f func()) {
	go func() {
		defer basex.Recover()
		f()
	}()
}
