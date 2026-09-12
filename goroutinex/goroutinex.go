package goroutinex

import (
	"context"
	"sync"

	"github.com/kanengo/ku/basex/recoveryx"
)

func GoSafe(f func()) {
	go func() {
		defer recoveryx.Recover()
		f()
	}()
}

func GoSafeWithCtx(ctx context.Context, f func(ctx context.Context)) {
	ctx = context.WithoutCancel(ctx)
	go func() {
		defer recoveryx.Recover()
		f(ctx)
	}()
}

func WaitGroup(fs ...func()) {
	var wg sync.WaitGroup
	wg.Add(len(fs))
	for _, f := range fs {
		GoSafe(func() {
			defer wg.Done()
			f()
		})
	}
	wg.Wait()
}
