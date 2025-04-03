package skiplist

import (
	"cmp"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func init() {
	// logrus.SetLevel(logrus.DebugLevel)
}

func TestBasic(t *testing.T) {
	s := New(CompareFunc(func(l, r []byte) int {
		lv, _ := strconv.ParseFloat(string(l), 64)

		rv, _ := strconv.ParseFloat(string(r), 64)
		return cmp.Compare(lv, rv)
	}))

	s.Insert([]byte("1.23"))
	s.Insert([]byte("-0.12"))
	s.Insert([]byte("4.56"))
	s.Insert([]byte("12.34"))
	s.Insert([]byte("0.12"))

	it := s.Iterator()
	it.SeekToFirst()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("-0.12"), it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("0.12"), it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("1.23"), it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("4.56"), it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("12.34"), it.Key())

	it.Next()
	assert.False(t, it.Valid())

	it.Seek([]byte("1.1"))
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("1.23"), it.Key())

	it.Seek([]byte("12.34"))
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("12.34"), it.Key())
}

func TestRandom(t *testing.T) {
	const (
		N    = 100_000
		seed = 0xa30378d2
	)

	rnd := rand.New(rand.NewSource(seed))
	s := New(CompareFunc(func(l, r []byte) int {
		lv, _ := strconv.Atoi(string(l))
		rv, _ := strconv.Atoi(string(r))
		return cmp.Compare(lv, rv)
	}))

	for range N {
		s.Insert([]byte(strconv.Itoa(rnd.Int())))
	}

	assert.Equal(t, N, s.Len())

	left := math.MinInt
	it := s.Iterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		v, _ := strconv.Atoi(string(it.Key()))
		assert.True(t, left <= v)
		left = v
	}

	right := math.MaxInt
	for it.SeekToLast(); it.Valid(); it.Prev() {
		v, _ := strconv.Atoi(string(it.Key()))
		assert.True(t, v <= right)
		right = v
	}
}

func generate(N int) []int {
	data := make([]int, N)
	for i := range N {
		data[i] = rand.Int()
	}
	return data
}
