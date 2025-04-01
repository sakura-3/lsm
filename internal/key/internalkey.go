package key

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"io"
)

type KeyType byte

const (
	KTypeDeletion KeyType = iota
	KTypeValue
)

// 对用户 key 的包装
type InternalKey struct {
	UserKey []byte
	Seq     uint64

	// 区分 delete 操作
	Type KeyType
}

func New(userKey []byte, seq uint64, tp KeyType) InternalKey {
	ik := InternalKey{
		UserKey: make([]byte, len(userKey)),
		Seq:     seq,
		Type:    tp,
	}
	copy(ik.UserKey, userKey)
	return ik
}

func (ik InternalKey) Size() uint64 {
	return uint64(len(ik.UserKey) + 8 + 1)
}

func (ik *InternalKey) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, uint16(len(ik.UserKey)))
	w.Write(ik.UserKey)
	binary.Write(w, binary.LittleEndian, ik.Seq)
	binary.Write(w, binary.LittleEndian, ik.Type)
	return nil
}

func (ik *InternalKey) DecodeFrom(r io.Reader) error {
	var Len uint16
	binary.Read(r, binary.LittleEndian, &Len)
	ik.UserKey = make([]byte, Len)
	r.Read(ik.UserKey)
	binary.Read(r, binary.LittleEndian, &ik.Seq)
	binary.Read(r, binary.LittleEndian, &ik.Type)
	return nil
}

// 按 UserKey 升序,Seq 降序
func InternalKeyCompareFunc(a, b any) int {
	ak := a.(InternalKey)
	bk := b.(InternalKey)
	return cmp.Or(
		bytes.Compare(ak.UserKey, bk.UserKey),
		-cmp.Compare(ak.Seq, bk.Seq),
	)
}
