package memtable

import (
	"bytes"
	"lsm/internal/key"
	"lsm/pkg/skiplist"
)

type Memtable struct {
	skl  *skiplist.Skiplist
	size uint64

	maxSize uint64
}

func NewMemtable(maxSize uint64) *Memtable {
	return &Memtable{
		skl:     skiplist.New(skiplist.CompareFunc(key.InternalKeyCompareFunc)),
		maxSize: maxSize,
	}
}

func (mem *Memtable) Add(seq uint64, tp key.KeyType, userKey []byte, userValue []byte) {
	ik := key.New(userKey, userValue, seq, tp)

	mem.skl.Insert(ik.EncodeTo())
	mem.size += ik.Size() + uint64(len(userValue))
}

// 返回 <= seq 的最新记录
func (mem *Memtable) Get(userKey []byte, seq uint64) (value []byte, ok bool) {
	// tp 值无意义
	expectKey := key.New(userKey, nil, seq, key.KTypeDeletion)
	iter := mem.skl.Iterator()
	iter.Seek(expectKey.EncodeTo())
	if !iter.Valid() {
		return nil, false
	}

	var exactKey key.InternalKey
	exactKey.DecodeFrom(iter.Key())
	// 只需要比较 userKey,seek 保证返回的是 seq 最大的记录
	if bytes.Equal(expectKey.UserKey, exactKey.UserKey) {
		switch exactKey.Type {
		case key.KTypeValue:
			return exactKey.UserValue, true
		case key.KTypeDeletion:
			return nil, false
		default:
			panic("unexpected type")
		}
	}
	return nil, false
}

func (mem *Memtable) Iterator() *skiplist.Iterator {
	return mem.skl.Iterator()
}

func (mem *Memtable) Full() bool {
	return mem.size >= mem.maxSize
}
