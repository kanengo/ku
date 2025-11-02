package basex

import (
	"runtime"

	"github.com/bytedance/gopkg/util/logger"
)

type panicHandler func(r interface{})

var buildPanicHandlers []panicHandler

func RegisterPanicHandler(handler panicHandler) {
	buildPanicHandlers = append(buildPanicHandlers, handler)
}

func Recover() {
	if r := recover(); r != nil {
		stack := make([]byte, 1024)
		runtime.Stack(stack, false)
		logger.Error("panic recover", "err", r, "stack", string(stack))
		for _, h := range buildPanicHandlers {
			h(r)
		}
	}
}
