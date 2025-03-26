package bloom

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloom(t *testing.T) {
	N := 10000
	bloom := NewBloom(uint64(N), 0.001)
	for i := 0; i < N; i++ {
		bloom.Add([]byte(strconv.Itoa(i)))
	}

	assert.True(t, bloom.Contains([]byte("1000")))
}

func BenchmarkBloomAdd(b *testing.B) {
	N := 1_000_000
	bloom := NewBloom(uint64(N), 0.001)
	data := make([]string, N)
	for i := range data {
		data[i] = strconv.Itoa(rand.Int())
	}
	for b.Loop() {
		idx := 0
		bloom.Add([]byte(data[idx]))
		idx++
		if idx == N {
			idx = 0
		}
	}
}

func BenchmarkBloomContains(b *testing.B) {
	N := 1_000_000
	bloom := NewBloom(uint64(N), 0.001)
	data := make([]string, N)
	for i := range data {
		data[i] = strconv.Itoa(rand.Int())
	}
	for i := 0; i < N; i++ {
		bloom.Add([]byte(data[i]))
	}

	for b.Loop() {
		idx := 0
		if !bloom.Contains([]byte(data[idx])) {
			b.Fail()
		}
		idx++
		if idx == N {
			idx = 0
		}
	}
}
