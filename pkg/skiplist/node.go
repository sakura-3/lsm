package skiplist

type node struct {
	level int
	key   any
	value any
	score float64
	next  []*node
	// prev 仅用于遍历, 只需要保存底层的 prev
	prev *node
}

func newNode(level int, key, value any, score float64) *node {
	return &node{
		level: level,
		key:   key,
		value: value,
		score: score,
		next:  make([]*node, level),
		prev:  nil,
	}
}
