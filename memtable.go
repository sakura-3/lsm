package lsm

import (
	"bytes"
	"lsm/internal/key"
	"lsm/pkg/skiplist"
)

type MemTable struct {
	skl  *skiplist.Skiplist
	size uint64

	maxSize uint64
}

func NewMemTable(maxSize uint64) *MemTable {
	return &MemTable{
		skl:     skiplist.New(skiplist.CompareFunc(key.InternalKeyCompareFunc)),
		maxSize: maxSize,
	}
}

func (mem *MemTable) Add(seq uint64, tp key.KeyType, userKey []byte, userValue []byte) {
	ik := key.New(userKey, userValue, seq, tp)

	mem.skl.Insert(ik)
	mem.size += ik.Size() + uint64(len(userValue))
}

// 返回 <= seq 的最新记录
func (mem *MemTable) Get(userKey []byte, seq uint64) (value []byte, ok bool) {
	// tp 值无意义
	expectKey := key.New(userKey, nil, seq, key.KTypeDeletion)
	iter := mem.skl.Iterator()
	iter.Seek(expectKey)
	if !iter.Valid() {
		return nil, false
	}

	exactKey := iter.Key().(key.InternalKey)
	// 只需要比较 userKey,seek 保证返回的是 seq 最大的记录
	if bytes.Equal(expectKey.UserKey, exactKey.UserKey) {
		switch exactKey.Type {
		case key.KTypeValue:
			ik := iter.Key().(key.InternalKey)
			return ik.UserValue, true
		case key.KTypeDeletion:
			return nil, false
		default:
			panic("unexpected type")
		}
	}
	return nil, false
}

func (mem *MemTable) Full() bool {
	return mem.size >= mem.maxSize
}
