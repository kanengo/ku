package elastic

import (
	"io"

	"github.com/kanengo/ku/bufferx/ring"
	rbPool "github.com/kanengo/ku/poolx/ringbuffer"
)

type RingBuffer struct {
	rb *rbPool.RingBuffer
}

//go:norace
func (b *RingBuffer) instance() *ring.Buffer {
	if b.rb == nil {
		b.rb = rbPool.Get()
	}
	return b.rb
}

//go:norace
func (b *RingBuffer) Done() {
	if b.rb != nil {
		rbPool.Put(b.rb)
		b.rb = nil
	}
}

//go:norace
func (b *RingBuffer) done() {
	if b.rb != nil && b.rb.IsEmpty() {
		rbPool.Put(b.rb)
		b.rb = nil
	}
}

//go:norace
func (b *RingBuffer) Peek(n int) (head []byte, tail []byte) {
	if b.rb == nil {
		return
	}
	return b.rb.Peek(n)
}

//go:norace
func (b *RingBuffer) Discard(n int) (int, error) {
	if b.rb == nil {
		return 0, ring.ErrIsEmpty
	}
	defer b.done()
	return b.rb.Discard(n)
}

//go:norace
func (b *RingBuffer) Read(p []byte) (n int, err error) {
	if b.rb == nil {
		return 0, ring.ErrIsEmpty
	}
	defer b.done()
	return b.rb.Read(p)
}

//go:norace
func (b *RingBuffer) ReadByte() (byte, error) {
	if b.rb == nil {
		return 0, ring.ErrIsEmpty
	}
	defer b.done()
	return b.rb.ReadByte()
}

//go:norace
func (b *RingBuffer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	return b.instance().Write(p)
}

//go:norace
func (b *RingBuffer) WriteByte(c byte) error {
	return b.instance().WriteByte(c)
}

//go:norace
func (b *RingBuffer) Buffered() int {
	if b.rb == nil {
		return 0
	}
	return b.rb.Buffered()
}

//go:norace
func (b *RingBuffer) Len() int {
	if b.rb == nil {
		return 0
	}
	return b.rb.Len()
}

// Cap returns the size of the underlying buffer.
//
//go:norace
func (b *RingBuffer) Cap() int {
	if b.rb == nil {
		return 0
	}
	return b.rb.Cap()
}

// Available returns the length of available bytes to write.
//
//go:norace
func (b *RingBuffer) Available() int {
	if b.rb == nil {
		return 0
	}
	return b.rb.Available()
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
//
//go:norace
func (b *RingBuffer) WriteString(s string) (int, error) {
	if len(s) == 0 {
		return 0, nil
	}
	return b.instance().WriteString(s)
}

// Bytes returns all available read bytes. It does not move the read pointer and only copy the available data.
//
//go:norace
func (b *RingBuffer) Bytes() []byte {
	if b.rb == nil {
		return nil
	}
	return b.rb.Bytes()
}

// ReadFrom implements io.ReaderFrom.
//
//go:norace
func (b *RingBuffer) ReadFrom(r io.Reader) (int64, error) {
	return b.instance().ReadFrom(r)
}

// WriteTo implements io.WriterTo.
//
//go:norace
func (b *RingBuffer) WriteTo(w io.Writer) (int64, error) {
	if b.rb == nil {
		return 0, ring.ErrIsEmpty
	}

	defer b.done()
	return b.instance().WriteTo(w)
}

// IsFull tells if this ring-buffer is full.
//
//go:norace
func (b *RingBuffer) IsFull() bool {
	if b.rb == nil {
		return false
	}
	return b.rb.IsFull()
}

// IsEmpty tells if this ring-buffer is empty.
//
//go:norace
func (b *RingBuffer) IsEmpty() bool {
	if b.rb == nil {
		return true
	}
	return b.rb.IsEmpty()
}

// Reset the read pointer and write pointer to zero.
//
//go:norace
func (b *RingBuffer) Reset() {
	if b.rb == nil {
		return
	}
	b.rb.Reset()
}
