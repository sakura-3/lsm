package skiplist

import (
	"cmp"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

const (
	maxLevel = 32
	p        = 0.5
)

type Skiplist struct {
	head  *node
	tail  *node
	seed  *rand.Rand
	size  int
	level int
	comp  comparable
}

func New(comp CompareFunc) *Skiplist {
	return &Skiplist{
		head:  newNode(maxLevel, nil, nil, -math.MaxFloat64),
		tail:  nil,
		seed:  rand.New(rand.NewSource(time.Now().UnixNano())),
		size:  0,
		level: 1,
		comp:  comp,
	}
}

func (s *Skiplist) Insert(key, value any) {
	h := s.head
	prev := make([]*node, maxLevel)
	for i := range prev {
		prev[i] = h
	}

	for i := s.level - 1; i >= 0; i-- {
		h = s.findLessThan(h, i, key)
		if h.next[i] != nil && s.compare(h.next[i], key) == 0 {
			panic(fmt.Sprintf("same key %v already exist in node %+v", key, h.next[i]))
		}
		prev[i] = h
	}

	newLevel := s.randomLevel()
	n := newNode(newLevel, key, value, s.comp.score(key))

	n.prev = prev[0]
	if prev[0].next[0] != nil {
		prev[0].next[0].prev = n
	}

	for i := range newLevel {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}

	if n.next[0] == nil {
		s.tail = n
	}
	s.level = max(s.level, newLevel)
	s.size++
}

func (s *Skiplist) Contains(key any) bool {
	h := s.head
	for i := s.level - 1; i >= 0; i-- {
		h = s.findLessThan(h, i, key)
		if h.next[i] != nil && s.compare(h.next[i], key) == 0 {
			return true
		}
	}

	return false
}

func (s *Skiplist) Iterator() *Iterator {
	return &Iterator{
		s:   s,
		cur: s.head,
	}
}

func (s *Skiplist) findLessThan(begin *node, level int, target any) *node {
	h := begin
	for next := h.next[level]; next != nil; next = h.next[level] {
		if s.compare(next, target) >= 0 {
			break
		}
		h = next
	}
	return h
}

func (s *Skiplist) Size() int {
	return s.size
}

func (s *Skiplist) Len() int {
	return s.size
}

func (s *Skiplist) randomLevel() int {
	level := 1
	for level < maxLevel && s.seed.Float64() < p {
		level++
	}
	return level
}

func (s *Skiplist) compare(n *node, key any) int {
	if res := cmp.Compare(n.score, s.comp.score(key)); res != 0 {
		return res
	}
	return s.comp.compare(n.key, key)
}

func (s *Skiplist) printLevel(i int) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("level %d:", i))
	sb.WriteString("head")
	for h := s.head; h.next[i] != nil; h = h.next[i] {
		sb.WriteString(fmt.Sprintf(" -> %v", h.next[i].key))
	}
	sb.WriteString(" -> nil")
	return sb.String()
}

func (s *Skiplist) String() string {
	var ss strings.Builder
	ss.WriteString(fmt.Sprintf("[level=%d,size=%d,", s.level, s.size))

	for i := s.level - 1; i >= 0; i-- {
		ss.WriteString(s.printLevel(i))
		ss.WriteByte('\n')
	}
	return ss.String()
}
