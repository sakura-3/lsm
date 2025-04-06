package sstable

import (
	"bytes"
	"encoding/binary"
	"lsm/internal/block"
	"lsm/internal/key"
	"os"

	"github.com/sirupsen/logrus"
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

	fileSize uint64

	offset uint32
	// 写入 internalKey + userValue
	dataBlockBuilder *block.BlockBuilder

	// 写入每个 dataBlock 的 最大 key 和 blockHandler
	// 使用 最大key 而不是 最小key 有助于 seek 的实现
	indexBlockBuilder *block.BlockBuilder

	// 若当前 data block 已满, hasPendingIndexEntry 设置为 true
	// 将在下次 Add 或 Finish 时为 index block 写入 maxKey:pendingIndexEntry
	hasPendingIndexEntry bool
	maxKey               []byte
	pendingIndexEntry    block.BlockHandler
}

func NewTableBuilder(filename string) (*TableBuilder, error) {
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
	if tb.hasPendingIndexEntry {
		tb.indexBlockBuilder.Add(tb.maxKey, tb.pendingIndexEntry.EncodeTo())
		tb.hasPendingIndexEntry = false
	}

	tb.maxKey = key

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

	if tb.hasPendingIndexEntry {
		tb.indexBlockBuilder.Add(tb.maxKey, tb.pendingIndexEntry.EncodeTo())
		tb.hasPendingIndexEntry = false
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
	footerData := footer.encodeTo()
	_, err = tb.fd.Write(footerData)
	tb.fileSize += uint64(footer.Size())
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

	tb.pendingIndexEntry = bh
	tb.hasPendingIndexEntry = true

	return nil
}

func (tb *TableBuilder) writeBlock(bb *block.BlockBuilder) (bh block.BlockHandler, err error) {
	data := bb.Finish()

	_, err = tb.fd.Write(data)
	tb.fileSize += uint64(len(data))
	if err != nil {
		return block.BlockHandler{}, err
	}

	bb.Reset()

	bh.Offset = tb.offset
	bh.Size = uint32(len(data))

	tb.offset += bh.Size

	return bh, nil
}

func (tb *TableBuilder) FileSize() uint64 {
	return tb.fileSize
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
	index, err := block.NewBlock(fd, footer.indexBlockHandler)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		fd:    fd,
		index: index,
	}, nil
}

func (s *SSTable) Get(lookupKey key.InternalKey) ([]byte, bool) {
	iter := s.NewIterator()
	iter.Seek(lookupKey.EncodeTo())
	if !iter.Valid() {
		return nil, false
	}

	var internalKey key.InternalKey
	internalKey.DecodeFrom(iter.Key())

	logrus.Debugf("lookupKey:%s,internalKey:%s\n", lookupKey.Debug(), internalKey.Debug())
	if !bytes.Equal(internalKey.UserKey, lookupKey.UserKey) {
		return nil, false
	}
	// TODO 引入 snapshot 后考虑是否可见

	if internalKey.Type == key.KTypeDeletion {
		return nil, false
	}

	// userKey 和 userValue 会合并为 internalKey
	// 作为 data block 的 key,对应的 value 为 nil
	return internalKey.UserValue, true
}

type SSTableIterator struct {
	sst *SSTable

	indexBlockIter *block.BlockIterator
	dataBlockIter  *block.BlockIterator
}

func (s *SSTable) NewIterator() *SSTableIterator {
	iter := &SSTableIterator{
		sst:            s,
		indexBlockIter: s.index.NewIterator(),
	}
	// load first data block
	if iter.indexBlockIter.Valid() {
		if err := iter.loadDataBlockFromIndex(); err != nil {
			panic(err)
		}
	}

	return iter
}

// seek to the first position where the key >= target
// Valid() is false after this call iff such position does not exist
// or some internal error occurs
func (si *SSTableIterator) Seek(target []byte) {
	si.dataBlockIter = nil

	// current data block has maxKey >= target,and prev data block's maxKey should < target
	// so the seek position should exactly in this block
	si.indexBlockIter.Seek(target)

	if !si.indexBlockIter.Valid() {
		return
	}

	// load data block
	if err := si.loadDataBlockFromIndex(); err != nil {
		panic(err)
	}

	si.dataBlockIter.Seek(target)
}

func (si *SSTableIterator) Valid() bool {
	return si.indexBlockIter.Valid() && (si.dataBlockIter != nil && si.dataBlockIter.Valid())
}

// requires si.Valid()
func (si *SSTableIterator) Key() []byte {
	return si.dataBlockIter.Key()
}

// requires si.Valid()
func (si *SSTableIterator) Value() []byte {
	return si.dataBlockIter.Value()
}

func (si *SSTableIterator) Next() {
	si.dataBlockIter.Next()
	if !si.dataBlockIter.Valid() {
		si.indexBlockIter.Next()
		if si.indexBlockIter.Valid() {
			if err := si.loadDataBlockFromIndex(); err != nil {
				panic(err)
			}
		}
	}
}

// require indexBlockIter.Valid()
func (si *SSTableIterator) loadDataBlockFromIndex() error {
	var bh block.BlockHandler
	bh.DecodeFrom(si.indexBlockIter.Value())
	dataBlock, err := block.NewBlock(si.sst.fd, bh)
	if err != nil {
		return err
	}
	si.dataBlockIter = dataBlock.NewIterator()
	return nil
}
