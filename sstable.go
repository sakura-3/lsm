package lsm

import (
	"bytes"
	"encoding/binary"
	"lsm/internal/block"
	"os"
)

const (
	// 4KB
	maxDataBlockSize = 4 * 1024
	magicNumber      = 0xdb4775248b80fb57
)

type Footer struct {
	// TODO
	// metaIndexBlockHandler block.BlockHandler
	indexBlockHandler block.BlockHandler
}

func (f Footer) Size() int {
	// blockHandler + magicNumber
	return 8 + 8
}

func (f *Footer) encodeTo() (data []byte) {
	data = f.indexBlockHandler.EncodeTo()
	data = binary.LittleEndian.AppendUint64(data, magicNumber)
	return data
}

func (f *Footer) decodeFrom(data []byte) {
	f.indexBlockHandler.DecodeFrom(data[:8])
	magic := binary.LittleEndian.Uint64(data[8:])
	if magic != magicNumber {
		panic("invalid magic number")
	}
}

// TODO meta block，如 bloom filter
type TableBuilder struct {
	fd *os.File

	offset uint32
	// 写入 internalKey + userValue
	dataBlockBuilder *block.BlockBuilder

	// 写入每个 dataBlock 的 最小 key 和 blockHandler
	indexBlockBuilder *block.BlockBuilder

	// 当前 dataBlock 的最小 key
	// dataBlock 写满后, 会把这个 key 写入 indexBlock
	minKey []byte
}

func newTableBuilder(filename string) (*TableBuilder, error) {
	fd, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &TableBuilder{
		fd:                fd,
		dataBlockBuilder:  block.NewBlockBuilder(),
		indexBlockBuilder: block.NewBlockBuilder(),
	}, nil
}

func (tb *TableBuilder) Add(key, value []byte) error {
	if tb.dataBlockBuilder.Empty() {
		tb.minKey = key
	}

	tb.dataBlockBuilder.Add(key, value)

	if tb.dataBlockBuilder.Size() >= maxDataBlockSize {
		if err := tb.flush(); err != nil {
			return err
		}
	}

	return nil
}

func (tb *TableBuilder) Finish() error {
	// write data block if any
	if err := tb.flush(); err != nil {
		return err
	}

	// index block
	bh, err := tb.writeBlock(tb.indexBlockBuilder)
	if err != nil {
		return err
	}

	// write footer
	footer := Footer{
		indexBlockHandler: bh,
	}
	_, err = tb.fd.Write(footer.encodeTo())
	if err != nil {
		return err
	}

	if err := tb.fd.Sync(); err != nil {
		return err
	}

	return tb.fd.Close()
}

func (tb *TableBuilder) flush() error {
	if tb.dataBlockBuilder.Empty() {
		return nil
	}

	bh, err := tb.writeBlock(tb.dataBlockBuilder)
	if err != nil {
		return err
	}
	tb.indexBlockBuilder.Add(tb.minKey, bh.EncodeTo())
	return nil
}

func (tb *TableBuilder) writeBlock(bb *block.BlockBuilder) (bh block.BlockHandler, err error) {
	data := bb.Finish()

	_, err = tb.fd.Write(data)
	if err != nil {
		return block.BlockHandler{}, err
	}

	bb.Reset()

	bh.Offset = tb.offset
	bh.Size = uint32(len(data))

	tb.offset += bh.Size

	return bh, nil
}

type SSTable struct {
	fd    *os.File
	index *block.Block
}

func Open(filename string) (*SSTable, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	info, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	// read footer
	var footer Footer
	footerData := make([]byte, footer.Size())
	if _, err := fd.ReadAt(footerData, info.Size()-int64(footer.Size())); err != nil {
		return nil, err
	}
	footer.decodeFrom(footerData)

	// load index block from footer
	indexData := make([]byte, footer.indexBlockHandler.Size)
	if _, err := fd.ReadAt(indexData, int64(footer.indexBlockHandler.Offset)); err != nil {
		return nil, err
	}
	index := block.NewBlock(indexData)

	return &SSTable{
		fd:    fd,
		index: index,
	}, nil
}

type SSTableIterator struct {
	sst *SSTable

	indexBlockIter *block.BlockIterator
	dataBlockIter  *block.BlockIterator
}

func (s *SSTable) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		sst:            s,
		indexBlockIter: s.index.NewIterator(),
	}
}

// 第一个 >= target 的位置, 只有不存在这样的记录时, Valid() 为 false
// seek to the first position where the key >= target
// Valid() is false after this call iff there 
func (si *SSTableIterator) Seek(target []byte) {
	si.dataBlockIter = nil

	// current data block has minKey >= target,so the seek position is
	// either at the beginning of the current data block or in the previous data block.
	si.indexBlockIter.Seek(target)

	if !si.indexBlockIter.Valid() {
		return
	}

	// if current data block's minKey == target,we can ensure that 
	// the seek position is at the beginning of the current data block.
	if bytes.Equal(si.indexBlockIter.Key(), target) {

	}
}

func (si *SSTableIterator) Valid() bool {
	return si.indexBlockIter.Valid() && (si.dataBlockIter != nil && si.dataBlockIter.Valid())
}
