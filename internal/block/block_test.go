package block

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlock(t *testing.T) {
	bb := NewBlockBuilder()
	bb.Add([]byte("key1"), []byte("value1"))
	bb.Add([]byte("key2"), []byte("value2"))
	bb.Add([]byte("key3"), []byte("value3"))

	data := bb.Finish()

	block := NewBlock(data)
	assert.Equal(t, 3, block.Size())

	iter := block.NewIterator()
	iter.Rewind()

	i := 1
	for iter.Valid() {
		key, value := iter.Key(), iter.Value()
		assert.Equal(t, key, []byte("key"+strconv.Itoa(i)))
		assert.Equal(t, value, []byte("value"+strconv.Itoa(i)))
		iter.Next()
		i++
	}

	iter.Rewind()

	iter.Seek([]byte("key2"))
	assert.Equal(t, iter.Key(), []byte("key2"))
	assert.Equal(t, iter.Value(), []byte("value2"))
}
