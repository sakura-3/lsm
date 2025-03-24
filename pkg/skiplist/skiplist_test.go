package skiplist

import (
	"cmp"
	"math"
	"math/rand"
	"testing"

	"github.com/huandu/skiplist"
	"github.com/stretchr/testify/assert"
)

func init() {
	// logrus.SetLevel(logrus.DebugLevel)
}

func TestBasic(t *testing.T) {
	s := New(CompareFunc(func(l, r any) int {
		return cmp.Compare(l.(float64), r.(float64))
	}))

	s.Insert(1.23, "2")
	s.Insert(-0.12, "0")
	s.Insert(4.56, "3")
	s.Insert(12.34, "4")
	s.Insert(0.12, "1")

	it := s.Iterator()
	it.SeekToFirst()
	assert.True(t, it.Valid())
	assert.Equal(t, -0.12, it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, 0.12, it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, 1.23, it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, 4.56, it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, 12.34, it.Key())

	it.Next()
	assert.False(t, it.Valid())
}

func TestRandom(t *testing.T) {
	const (
		N    = 100_000
		seed = 0xa30378d2
	)

	rnd := rand.New(rand.NewSource(seed))
	s := New(CompareFunc(func(l, r any) int {
		return cmp.Compare(l.(int), r.(int))
	}))

	for i := range N {
		s.Insert(rnd.Int(), i)
	}

	assert.Equal(t, N, s.Len())

	left := math.MinInt
	it := s.Iterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		assert.True(t, left <= it.Key().(int))
		left = it.Key().(int)
	}

	right := math.MaxInt
	for it.SeekToLast(); it.Valid(); it.Prev() {
		assert.True(t, it.Key().(int) <= right)
		right = it.Key().(int)
	}
}

func generate(N int) []int {
	data := make([]int, N)
	for i := range N {
		data[i] = rand.Int()
	}
	return data
}

func BenchmarkMySkiplistWrite(b *testing.B) {
	s := New(CompareFunc(func(l, r any) int {
		return cmp.Compare(l.(int), r.(int))
	}))

	N := 1_000_000
	data := generate(N)

	idx := 0
	for b.Loop() {
		s.Insert(data[idx], idx)
		idx++
		if idx == N {
			idx = 0
		}
	}
}

func BenchmarkMySkiplistRead(b *testing.B) {
	s := New(CompareFunc(func(l, r any) int {
		return cmp.Compare(l.(int), r.(int))
	}))
	N := 1_000_000
	data := generate(N)
	for i := range N {
		s.Insert(data[i], i)
	}

	it := s.Iterator()
	idx := 0
	for b.Loop() {
		it.Seek(data[idx])
		_ = it.Value()
		idx++
		if idx == N {
			idx = 0
		}
	}
}

func BenchmarkSkiplistWrite(b *testing.B) {
	s := skiplist.New(skiplist.Int)
	N := 1_000_000
	data := generate(N)

	idx := 0
	for b.Loop() {
		s.Set(data[idx], idx)
		idx++
		if idx == N {
			idx = 0
		}
	}
}

func BenchmarkSkiplistRead(b *testing.B) {
	s := skiplist.New(skiplist.Int)
	N := 1_000_000
	data := generate(N)
	for i := range N {
		s.Set(data[i], i)
	}

	idx := 0
	for b.Loop() {
		_ = s.Get(data[idx])
		idx++
		if idx == N {
			idx = 0
		}
	}
}
