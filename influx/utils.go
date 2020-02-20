package influx

import (
	"bytes"
	"sync"
)

func NewBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

type bufferPool struct {
	pool sync.Pool
}

type BufferPool interface {
	Get() *bytes.Buffer
	Put(buf *bytes.Buffer)
}

func (b *bufferPool) Get() *bytes.Buffer {
	return b.pool.Get().(*bytes.Buffer)
}

func (b *bufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	b.pool.Put(buf)
}
