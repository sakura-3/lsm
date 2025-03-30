package block

import (
	"bytes"
	"encoding/binary"
)

type BlockBuilder struct {
	buf     *bytes.Buffer
	counter uint32
}

func NewBlockBuilder() *BlockBuilder {
	return &BlockBuilder{
		buf:     &bytes.Buffer{},
		counter: 0,
	}
}

func (b *BlockBuilder) Add(key, value []byte) {
	binary.Write(b.buf, binary.LittleEndian, uint32(len(key)))
	b.buf.Write(key)
	binary.Write(b.buf, binary.LittleEndian, uint32(len(value)))
	b.buf.Write(value)
	b.counter++
}

func (b *BlockBuilder) Finish() []byte {
	binary.Write(b.buf, binary.LittleEndian, b.counter)
	return b.buf.Bytes()
}

func (b *BlockBuilder) Reset() {
	b.counter = 0
	b.buf.Reset()
}

func (b *BlockBuilder) Size() int {
	return b.buf.Len()
}

func (b *BlockBuilder) Empty() bool {
	return b.buf.Len() == 0
}
