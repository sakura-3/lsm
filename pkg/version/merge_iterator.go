package version

import (
	"container/heap"
	"lsm/internal/key"
	"lsm/pkg/sstable"
)

// 用于合并多个有序的SSTable文件
type mergeIterator struct {
	iterators []*sstable.SSTableIterator
	hp        hp
}

func NewMergeIterator(iterators []*sstable.SSTableIterator) *mergeIterator {
	mi := &mergeIterator{
		iterators: iterators,
		hp:        make(hp, 0),
	}
	for i, it := range iterators {
		if it.Valid() {
			heap.Push(&mi.hp, info{
				k:   it.Key(),
				idx: i,
			})
		}
	}
	return mi
}

func (it *mergeIterator) Valid() bool {
	return it.hp.Len() > 0
}

func (it *mergeIterator) Key() []byte {
	idx := it.hp[0].idx
	return it.iterators[idx].Key()
}

func (it *mergeIterator) Next() {
	idx := it.hp[0].idx
	it.iterators[idx].Next()
	heap.Pop(&it.hp)
	if it.iterators[idx].Valid() {
		heap.Push(&it.hp, info{
			k:   it.iterators[idx].Key(),
			idx: idx,
		})
	}
}

type info struct {
	k   []byte
	idx int
}
type hp []info

func (h hp) Len() int           { return len(h) }
func (h hp) Less(i, j int) bool { return key.InternalKeyCompareFunc(h[i].k, h[j].k) < 0 }
func (h hp) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hp) Push(x any) {
	*h = append(*h, x.(info))
}
func (h *hp) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
