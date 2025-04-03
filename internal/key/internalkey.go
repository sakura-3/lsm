package key

import (
	"bytes"
	"cmp"
	"encoding/binary"
)

type KeyType byte

const (
	KTypeDeletion KeyType = iota
	KTypeValue
)

// 对用户 KV 的包装
type InternalKey struct {
	UserKey   []byte
	UserValue []byte
	Seq       uint64

	// 区分 delete 操作
	Type KeyType
}

func New(userKey, userValue []byte, seq uint64, tp KeyType) InternalKey {
	ik := InternalKey{
		UserKey:   make([]byte, len(userKey)),
		UserValue: make([]byte, len(userValue)),
		Seq:       seq,
		Type:      tp,
	}
	copy(ik.UserKey, userKey)
	copy(ik.UserValue, userValue)
	return ik
}

func (ik InternalKey) Size() uint64 {
	return 4 + uint64(len(ik.UserKey)) + // userKey
		4 + uint64(len(ik.UserValue)) + // userValue
		8 + // seq
		1 // type
}

func (ik *InternalKey) EncodeTo() []byte {
	data := make([]byte, ik.Size())
	keyLen, valueLen := uint32(len(ik.UserKey)), uint32(len(ik.UserValue))
	offset := 0

	binary.LittleEndian.PutUint32(data, keyLen)
	offset += 4

	copy(data[offset:], ik.UserKey)
	offset += int(keyLen)

	binary.LittleEndian.PutUint32(data[offset:], valueLen)
	offset += 4

	copy(data[offset:], ik.UserValue)
	offset += int(valueLen)

	binary.LittleEndian.PutUint64(data[offset:], ik.Seq)
	offset += 8
	data[offset] = byte(ik.Type)
	offset += 1

	if offset != len(data) {
		panic("offset != len(data)")
	}

	return data
}

func (ik *InternalKey) DecodeFrom(data []byte) {
	offset := 0
	kLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	ik.UserKey = make([]byte, kLen)
	copy(ik.UserKey, data[offset:])
	offset += int(kLen)

	vLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	ik.UserValue = make([]byte, vLen)
	copy(ik.UserValue, data[offset:])
	offset += int(vLen)

	ik.Seq = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	ik.Type = KeyType(data[offset])
	offset += 1
	if offset != len(data) {
		panic("offset!= len(data)")
	}
}

// 按 UserKey 升序,Seq 降序
func InternalKeyCompareFunc(a, b []byte) int {
	var ak, bk InternalKey
	ak.DecodeFrom(a)
	bk.DecodeFrom(b)
	return cmp.Or(
		bytes.Compare(ak.UserKey, bk.UserKey),
		-cmp.Compare(ak.Seq, bk.Seq),
	)
}
