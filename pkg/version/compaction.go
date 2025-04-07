package version

import (
	"bytes"
	"fmt"
	"lsm/internal/key"
	"lsm/internal/util"
	"lsm/pkg/sstable"
	"strings"

	"github.com/sirupsen/logrus"
)

// compact sstable file inputs[0] at level with inputs[1] at level+1
type compaction struct {
	level  int
	inputs [2][]*FileMetaData
}

func (c *compaction) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("compaction,level:%d\n", c.level))

	sb.WriteString("inputs[0]:")
	for _, f := range c.inputs[0] {
		sb.WriteString(fmt.Sprintf("%d,", f.number))
	}
	sb.WriteString("\n")

	sb.WriteString("inputs[1]:")
	for _, f := range c.inputs[1] {
		sb.WriteString(fmt.Sprintf("%d,", f.number))
	}
	sb.WriteString("\n")
	return sb.String()
}

func (c *compaction) isTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

// copy from leveldb/db/version_set.cc VersionSet::Finalize()
func (v *Version) pickCompactionLevel() int {
	// We treat level-0 specially by bounding the number of files
	// instead of number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too
	// many level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and
	// therefore we wish to avoid too many files when the individual
	// file size is small (perhaps because of a small write-buffer
	// setting, or very high compression ratios, or lots of
	// overwrites/deletions).
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0
	for level := range DefaultLevels {
		if level == 0 {
			score = float64(len(v.files[0])) / float64(L0_CompactionTrigger)
		} else {
			score = float64(totalFileSize(v.files[level])) / maxBytesForLevel(level)
		}

		if score > bestScore {
			bestScore = score
			compactionLevel = level
		}

	}
	return compactionLevel
}

func (v *Version) pickCompaction() *compaction {
	level := v.pickCompactionLevel()
	if level < 0 {
		return nil
	}
	c := &compaction{
		level: level,
	}

	// set inputs[0]

	var smallest, largest []byte
	// files in level 0 is not sorted globally
	// so pick up all
	if level == 0 {
		c.inputs[0] = append(c.inputs[0], v.files[0]...)
		for _, f := range c.inputs[0] {
			if smallest == nil || key.InternalKeyCompareFunc(f.smallest.EncodeTo(), smallest) < 0 {
				smallest = f.smallest.EncodeTo()
			}
			if largest == nil || key.InternalKeyCompareFunc(f.largest.EncodeTo(), largest) > 0 {
				largest = f.largest.EncodeTo()
			}
		}
	} else {
		// files in other level is sorted globally
		// pick the first file that comes after compactPointer
		for i := range v.files[level] {
			if v.compactPointer[level] == nil || key.InternalKeyCompareFunc(v.files[level][i].largest.EncodeTo(), v.compactPointer[level]) > 0 {
				c.inputs[0] = append(c.inputs[0], v.files[level][i])
				break
			}
		}
		if len(c.inputs[0]) == 0 {
			c.inputs[0] = append(c.inputs[0], v.files[level][0])
		}
		smallest = c.inputs[0][0].smallest.EncodeTo()
		largest = c.inputs[0][0].largest.EncodeTo()
	}

	// set inputs[1]
	for _, f := range v.files[level+1] {
		if key.InternalKeyCompareFunc(f.largest.EncodeTo(), smallest) < 0 || key.InternalKeyCompareFunc(f.smallest.EncodeTo(), largest) > 0 {
			// not overlap at all
		} else {
			c.inputs[1] = append(c.inputs[1], f)
		}
	}

	return c
}

// 返回 inputs 经过合并后的结果
// 合并后的数据会被写入到 level+1 中
// 同时 inputs 中的文件会被删除
func (v *Version) getCompactOutput(c *compaction) ([]*FileMetaData, error) {
	// just move file from c.level to c.level+1
	if c.isTrivialMove() {
		return c.inputs[0], nil
	}

	iters := make([]*sstable.SSTableIterator, 0, len(c.inputs[0])+len(c.inputs[1]))
	for _, f := range c.inputs[0] {
		st, err := f.Load()
		if err != nil {
			return nil, err
		}
		iters = append(iters, st.NewIterator())
	}

	for _, f := range c.inputs[1] {
		st, err := f.Load()
		if err != nil {
			return nil, err
		}
		iters = append(iters, st.NewIterator())
	}

	mi := NewMergeIterator(iters)

	var (
		currentKey *key.InternalKey = nil
		metas                       = make([]*FileMetaData, 0)
	)
	for ; mi.Valid(); mi.Next() {
		meta := &FileMetaData{
			allowSeeks: 1 << 30,
			number:     v.nextFileNumber,
			smallest:   new(key.InternalKey),
			largest:    new(key.InternalKey),
		}
		v.nextFileNumber++
		meta.smallest.DecodeFrom(mi.Key())

		builder, err := sstable.NewTableBuilder(util.SstableFileName(v.dbName, meta.number))
		if err != nil {
			return nil, err
		}

		for ; mi.Valid(); mi.Next() {
			var nextKey key.InternalKey
			nextKey.DecodeFrom(mi.Key())
			if currentKey != nil {
				compareResult := bytes.Compare(currentKey.UserKey, nextKey.UserKey)
				if compareResult == 0 {
					// 重复的记录
					// 注意 记录是按照 userKey 升序,seq 降序排列的
					// userKey 相同时，第一条记录是最新的,只需要保留最新的记录
					// TODO 考虑 snapshot,则此处应该保留所有 >= snapshot.seq 的记录
					continue
				} else if compareResult < 0 {
					logrus.Fatalf("%s < %s", string(currentKey.UserKey), string(nextKey.UserKey))
				}
				currentKey = &nextKey
			} else {
				currentKey = &nextKey
			}
			meta.largest.DecodeFrom(mi.Key())
			builder.Add(mi.Key(), nil)

			// 这里的 FileSize 只是估计值, 实际值更大
			if builder.FileSize() > MaxSSTableFileSize {
				break
			}
		}
		if err := builder.Finish(); err != nil {
			return nil, err
		}
		// 这里的 FileSize 是准确值
		meta.fileSize = builder.FileSize()
		metas = append(metas, meta)
	}

	return metas, nil
}

// major compact
func (v *Version) compact() bool {
	c := v.pickCompaction()
	if c == nil {
		return false
	}

	logrus.Debugf("compact begin")
	logrus.Debug(c.String())

	compactOutput, err := v.getCompactOutput(c)
	if err != nil {
		logrus.Errorf("getCompactOutput failed, err:%v", err)
		return false
	}

	for _, f := range c.inputs[0] {
		v.deleteFile(c.level, f)
	}

	for _, f := range c.inputs[1] {
		v.deleteFile(c.level+1, f)
	}

	for _, f := range compactOutput {
		v.addFile(c.level+1, f)
	}

	return true
}
