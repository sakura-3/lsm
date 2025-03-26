package wal

import (
	"bytes"
	"sync"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var blockPool = sync.Pool{
	New: func() any {
		return make([]byte, blockSize)
	},
}

func getBlock() []byte {
	return blockPool.Get().([]byte)
}

func putBlock(b []byte) {
	blockPool.Put(b)
}

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(b *bytes.Buffer) {
	b.Reset()
	bufferPool.Put(b)
}
