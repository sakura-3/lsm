package block

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
)

type BlockHandler struct {
	Offset uint32
	Size   uint32
}

func (bh *BlockHandler) EncodeTo() []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, bh.Offset)
	binary.LittleEndian.PutUint32(data[4:], bh.Size)
	return data
}

func (bh *BlockHandler) DecodeFrom(data []byte) {
	bh.Offset = binary.LittleEndian.Uint32(data)
	bh.Size = binary.LittleEndian.Uint32(data[4:])
}

type Block struct {
	keys   [][]byte
	values [][]byte
}

func NewBlock(fd *os.File, bh BlockHandler) (*Block, error) {
	data := make([]byte, bh.Size)
	if _, err := fd.ReadAt(data, int64(bh.Offset)); err != nil {
		return nil, err
	}
	return newBlockFromRawData(data), nil
}

func newBlockFromRawData(data []byte) *Block {
	counter := binary.LittleEndian.Uint32(data[len(data)-4:])

	block := &Block{
		keys:   make([][]byte, counter),
		values: make([][]byte, counter),
	}

	offset := 0
	for i := range counter {
		keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		key := data[offset : offset+int(keyLen)]
		offset += int(keyLen)
		valueLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		value := data[offset : offset+int(valueLen)]
		offset += int(valueLen)

		block.keys[i] = key
		block.values[i] = value
	}

	return block
}

func (b *Block) Size() int {
	return len(b.keys)
}

type BlockIterator struct {
	block *Block
	index int
}

func (b *Block) NewIterator() *BlockIterator {
	return &BlockIterator{
		block: b,
		index: 0,
	}
}

func (bi *BlockIterator) Rewind() {
	bi.index = 0
}

// seek to the first position where the key >= target
// Valid() is false after this call iff such position does not exist
func (bi *BlockIterator) Seek(target []byte) {
	idx := sort.Search(len(bi.block.keys), func(i int) bool {
		return bytes.Compare(bi.block.keys[i], target) >= 0
	})
	bi.index = idx
}

func (bi *BlockIterator) Next() {
	bi.index++
}

func (bi *BlockIterator) Prev() {
	bi.index--
}

func (bi *BlockIterator) Valid() bool {
	return bi.index < len(bi.block.keys) && bi.index >= 0
}

// requires bi.Valid()
func (bi *BlockIterator) Key() []byte {
	return bi.block.keys[bi.index]
}

// requires bi.Valid()
func (bi *BlockIterator) Value() []byte {
	return bi.block.values[bi.index]
}

func (bi *BlockIterator) Close() {
	bi.block = nil
}
