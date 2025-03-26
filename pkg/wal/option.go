package wal

// TODO 设置 segment 文件的数量
type Option struct {
	Dir         string
	SegmentSize uint64

	// 是否在每次写入后都 sync
	// 设置为 false 会提高性能,但不能保证持久性
	Sync bool
}

var DefaultOptions = Option{
	Dir:         "./wal",
	SegmentSize: 1 * GB,
	Sync:        true,
}
