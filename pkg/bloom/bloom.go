package bloom

import (
	"hash/fnv"
	"math"
)

type BitArray []byte

func newBitArray(size uint64) BitArray {
	return make(BitArray, (size+7)/8)
}

func (b BitArray) set(idx uint64) {
	b[idx/8] |= 1 << (idx % 8)
}

func (b BitArray) get(idx uint64) bool {
	return b[idx/8]&(1<<(idx%8)) != 0
}

type Bloom struct {
	bitArray BitArray

	// 哈希函数个数
	k uint64

	// 位数组长度
	m uint64
}

// n 代表预期的元素个数
// p 代表错误率, 当布隆过滤器判断某个元素存在时，实际上该元素并不在集合中的概率
func NewBloom(n uint64, p float64) *Bloom {
	m := uint64(-(float64(n) * math.Log(p)) / (math.Log(2) * math.Log(2)))
	k := uint64((float64(m) / float64(n)) * math.Log(2))
	return &Bloom{
		bitArray: newBitArray(m),
		k:        k,
		m:        m,
	}
}

func (b *Bloom) hash(key []byte) []uint64 {
	hsh := make([]uint64, b.k)
	for i := range hsh {
		h := fnv.New64a()
		h.Write(key)
		h.Write([]byte{byte(i)})
		hsh[i] = h.Sum64() % b.m
	}
	return hsh
}

func (b *Bloom) Add(key []byte) {
	hsh := b.hash(key)
	for _, idx := range hsh {
		b.bitArray.set(idx)
	}
}

func (b *Bloom) Contains(key []byte) bool {
	hsh := b.hash(key)
	for _, idx := range hsh {
		if !b.bitArray.get(idx) {
			return false
		}
	}
	return true
}
