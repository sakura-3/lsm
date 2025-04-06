package sstable

import (
	"bytes"
	"fmt"
	"io"
	"lsm/internal/block"
	"lsm/internal/key"
	"math"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	// logrus.SetLevel(logrus.DebugLevel)
	logrus.SetLevel(logrus.FatalLevel)
}

func TestFooterEncode(t *testing.T) {
	f := Footer{
		indexBlockHandler: block.BlockHandler{
			Offset: 8,
			Size:   16,
		},
	}
	actual := f.encodeTo()
	expected := []byte{
		// offset
		0x8, 0x0, 0x0, 0x0,
		// size
		0x10, 0x0, 0x0, 0x0,
		// magic number
		0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb,
	}
	assert.Equal(t, expected, actual)
}

func TestFooterDecode(t *testing.T) {
	f := Footer{}
	f.decodeFrom([]byte{
		// offset
		0x78, 0x56, 0x34, 0x12,
		// size
		0x12, 0x34, 0x56, 0x78,
		// magic number
		0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb,
	})
	assert.Equal(t, uint32(0x12345678), f.indexBlockHandler.Offset)
	assert.Equal(t, uint32(0x78563412), f.indexBlockHandler.Size)
}

func TestSSTableBasic(t *testing.T) {
	tb, err := NewTableBuilder("TestSSTableBasic.sst")
	assert.Nil(t, err)
	defer os.Remove("TestSSTableBasic.sst")

	// key1 value
	tb.Add([]byte{0x1, 0x1, 0x1, 0x1}, []byte{0x01, 0x23, 0x45, 0x67, 0x89})
	// key2 value
	tb.Add([]byte{0x2, 0x2, 0x2, 0x2, 0x2}, []byte{0x01, 0x23, 0x45, 0x67, 0x89})
	// key3 value
	tb.Add([]byte{0x3, 0x3, 0x3, 0x3, 0x3, 0x3}, []byte{0x01, 0x23, 0x45, 0x67, 0x89})

	tb.Finish()

	// 打开文件
	fd, err := os.Open("TestSSTableBasic.sst")
	assert.Nil(t, err)
	defer fd.Close()
	actual, err := io.ReadAll(fd)
	assert.Nil(t, err)

	expected := []byte{
		// data block 1 begin

		// len(key1) = 4, little endian
		0x4, 0x0, 0x0, 0x0,
		// key1 = 0x01010101
		0x01, 0x01, 0x01, 0x01,
		// len(value) = 5, little endian
		0x5, 0x0, 0x0, 0x0,
		// value = 0x0123456789
		0x01, 0x23, 0x45, 0x67, 0x89,

		// len(key2) = 5, little endian
		0x5, 0x0, 0x0, 0x0,
		// key2 = 0x0202020202
		0x02, 0x02, 0x02, 0x02, 0x02,
		// len(value) = 5, little endian
		0x5, 0x0, 0x0, 0x0,
		// value = 0x0123456789
		0x01, 0x23, 0x45, 0x67, 0x89,

		// len(key3) = 6, little endian
		0x6, 0x0, 0x0, 0x0,
		// key3 = 0x030303030303
		0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
		// len(value) = 5, little endian
		0x5, 0x0, 0x0, 0x0,
		// value = 0x0123456789
		0x01, 0x23, 0x45, 0x67, 0x89,

		// counter = 3, little endian
		0x3, 0x0, 0x0, 0x0,

		// data block 1 end

		// index block begin

		// len(key3) = 6, little endian
		0x6, 0x0, 0x0, 0x0,
		// key3 = 0x030303030303
		0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
		// len(value) = 8, little endian
		0x8, 0x0, 0x0, 0x0,
		// value = blockHandler{offset: 0, size: 58} -> [0x0, 0x0, 0x0, 0x0, 0x3a, 0x0, 0x0, 0x0]
		0x0, 0x0, 0x0, 0x0, 0x3a, 0x0, 0x0, 0x0,

		// counter = 1, little endian
		0x1, 0x0, 0x0, 0x0,

		// index block end

		// footer begin

		// index block handler = blockHandler{offset: 58, size: 26} -> [0x3a, 0x0, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0]
		0x3a, 0x0, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0,
		// magic number, little endian
		0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb,

		// footer end
	}

	assert.Equal(t, expected, actual)
}

func TestSSTableMultipleDataBlock(t *testing.T) {
	tb, err := NewTableBuilder("TestSSTableMultipleDataBlock.sst")
	assert.Nil(t, err)
	defer os.Remove("TestSSTableMultipleDataBlock.sst")

	// a data block can hold maxDataBlockSize(4096) / (4+12+4+12) = 128 key-value pairs
	keyFormat := "k%11d"
	valueFormat := "v%11d"

	// should take ceil(keyN / 128) = 782 data blocks
	// so we should have 782 kv in index block
	keyN := 100_000
	for i := range keyN {
		tb.Add(fmt.Appendf(nil, keyFormat, i), fmt.Appendf(nil, valueFormat, i))
	}
	tb.Finish()

	sstable, err := Open("TestSSTableMultipleDataBlock.sst")
	assert.Nil(t, err)

	assert.Equal(t, 782, sstable.index.Size())
}

func TestSSTableGet(t *testing.T) {
	tb, err := NewTableBuilder("TestSSTableGet.sst")
	assert.Nil(t, err)
	defer os.Remove("TestSSTableGet.sst")

	userKeyFormat := "key-%10d"
	userValueFormat := "value-%10d"
	keyN := 10
	deleteKeyN := keyN / 10
	deleteMap := make(map[int]struct{})
	for range deleteKeyN {
		rd := rand.IntN(keyN)
		deleteMap[rd] = struct{}{}
	}

	for i := range keyN {
		userKey := fmt.Appendf(nil, userKeyFormat, i)
		userValue := fmt.Appendf(nil, userValueFormat, i)
		keyForInsert := key.New(userKey, userValue, uint64(i), key.KTypeValue)
		err := tb.Add(keyForInsert.EncodeTo(), nil)
		assert.Nil(t, err)

		// 如果对同一个 key 的插入和删除使用同一个 seq,是否可见?
		if _, ok := deleteMap[i]; ok {
			keyForDelete := key.New(userKey, nil, uint64(i+1), key.KTypeDeletion)
			err := tb.Add(keyForDelete.EncodeTo(), nil)
			assert.Nil(t, err)
		}
	}
	tb.Finish()

	st, err := Open("TestSSTableGet.sst")
	assert.Nil(t, err)

	iter := st.NewIterator()
	assert.True(t, iter.Valid())
	for ; iter.Valid(); iter.Next() {
		var internalKey key.InternalKey
		internalKey.DecodeFrom(iter.Key())
		t.Logf("key: %s, value: %s,seq: %d,type:%d\n", internalKey.UserKey, internalKey.UserValue, internalKey.Seq, internalKey.Type)
	}

	for i := range keyN {
		lookupKey := key.NewLookupKey(fmt.Appendf(nil, userKeyFormat, i), math.MaxUint64)
		expected := fmt.Appendf(nil, userValueFormat, i)

		actual, ok := st.Get(lookupKey)
		if _, delete := deleteMap[i]; delete {
			// t.Log(string(actual))
			// var actualKey key.InternalKey
			// actualKey.DecodeFrom(actual)
			// t.Logf("key: %s, value: %s,seq: %d,type:%d\n", actualKey.UserKey, actualKey.UserValue, actualKey.Seq, actualKey.Type)
			assert.False(t, ok)
			continue
		}
		assert.True(t, ok)
		// assert.Equal(t, expected, actual)
		if !bytes.Equal(expected, actual) {
			t.Errorf("expected: %s, actual: %s\n", expected, actual)
		}
	}
}
