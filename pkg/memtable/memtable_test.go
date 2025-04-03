package memtable

import (
	"lsm/internal/key"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemTableBasic(t *testing.T) {
	mem := NewMemtable(math.MaxUint64)
	mem.Add(0, key.KTypeValue, []byte("name"), []byte("xiao ming"))
	mem.Add(1, key.KTypeValue, []byte("age"), []byte("18"))
	mem.Add(2, key.KTypeValue, []byte("name"), []byte("hong hong"))
	mem.Add(3, key.KTypeDeletion, []byte("name"), nil)

	v1, ok := mem.Get([]byte("name"), 0)
	assert.True(t, ok)
	assert.Equal(t, []byte("xiao ming"), v1)

	v2, ok := mem.Get([]byte("age"), 1)
	assert.True(t, ok)
	assert.Equal(t, []byte("18"), v2)

	v2_1, ok := mem.Get([]byte("age"), 100)
	assert.True(t, ok)
	assert.Equal(t, []byte("18"), v2_1)

	v3, ok := mem.Get([]byte("name"), 2)
	assert.True(t, ok)
	assert.Equal(t, v3, []byte("hong hong"))

	// seq>=3 时，name 记录被删除了
	v4, ok := mem.Get([]byte("name"), 3)
	assert.False(t, ok)
	assert.Nil(t, v4)

	v5, ok := mem.Get([]byte("name"), 100)
	assert.False(t, ok)
	assert.Nil(t, v5)

	// seq=0 时, age 还不可见
	v6, ok := mem.Get([]byte("age"), 0)
	assert.False(t, ok)
	assert.Nil(t, v6)
}
