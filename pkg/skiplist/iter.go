package skiplist

type Iterator struct {
	s   *Skiplist
	cur *node
}

// return false if cur is nil or s.head
func (it *Iterator) Valid() bool {
	return it.cur != nil && it.cur.key != nil
}

func (it *Iterator) Key() any {
	if !it.Valid() {
		return nil
	}
	return it.cur.key
}

func (it *Iterator) Value() any {
	if !it.Valid() {
		return nil
	}
	return it.cur.value
}

func (it *Iterator) Next() {
	if !it.Valid() {
		panic("Iterator is not valid")
	}
	it.cur = it.cur.next[0]
}

func (it *Iterator) Prev() {
	if !it.Valid() {
		panic("Iterator is not valid")
	}
	it.cur = it.cur.prev
}

// seek to first node that >= target
func (it *Iterator) Seek(target any) {
	h := it.s.head
	for i := it.s.level - 1; i >= 0; i-- {
		h = it.s.findLessThan(h, i, target)
	}
	it.cur = h.next[0]
}

func (it *Iterator) SeekToFirst() {
	it.cur = it.s.head.next[0]
}

func (it *Iterator) SeekToLast() {
	if it.s.tail == nil {
		it.cur = nil
	}
	it.cur = it.s.tail.prev
}
