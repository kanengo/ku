package bytebuffer

import (
	"github.com/valyala/bytebufferpool"
)

type ByteBuffer = bytebufferpool.ByteBuffer

var (
	Get = bytebufferpool.Get

	Put = func(b *ByteBuffer) {
		if b != nil {
			bytebufferpool.Put(b)
		}
	}
)
