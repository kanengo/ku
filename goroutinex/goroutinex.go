package goroutinex

import (
	"github.com/kanengo/ku/basex/recoveryx"
)

func GoSafe(f func()) {
	go func() {
		defer recoveryx.Recover()
		f()
	}()
}
