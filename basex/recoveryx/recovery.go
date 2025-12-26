package recoveryx

import (
	"log/slog"
	"runtime"
)

type PanicHandler func(r any)

var buildPanicHandlers []PanicHandler

func RegisterPanicHandler(handler PanicHandler) {
	buildPanicHandlers = append(buildPanicHandlers, handler)
}

func Recover() {
	if r := recover(); r != nil {
		stack := make([]byte, 1024)
		n := runtime.Stack(stack, false)
		stack = stack[:n]
		slog.Error("panic recover", "err", r, "stack", string(stack))
		for _, h := range buildPanicHandlers {
			h(r)
		}
	}
}

func Revocery(handlers ...PanicHandler) func() {
	return func() {
		if r := recover(); r != nil {
			stack := make([]byte, 1024)
			n := runtime.Stack(stack, false)
			stack = stack[:n]
			slog.Error("panic recover", "err", r, "stack", string(stack))
			for _, h := range handlers {
				h(r)
			}
			for _, h := range buildPanicHandlers {
				h(r)
			}
		}
	}
}
