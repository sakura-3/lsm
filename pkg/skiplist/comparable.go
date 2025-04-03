package skiplist

// 用户一般使用 CompareFunc，对应的 score 为0
// skiplist 内部先比较 score, 再使用 Compare
// 这允许我们定义两个 dummy node, 即 head 和 nilNode
// 对于所有合法 node， head < node < nilNode
type comparable interface {
	compare(l, r []byte) int
	score(key []byte) float64
}

type CompareFunc func(l, r []byte) int

func (f CompareFunc) compare(l, r []byte) int {
	return f(l, r)
}
func (f CompareFunc) score(key []byte) float64 {
	return 0
}
