package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"lsm/internal/key"
	"os"
	"slices"
	"sort"

	"github.com/sirupsen/logrus"
)

// represent a sstable file in the disk
type FileMetaData struct {
	// Seeks allowed until compaction in leveldb
	// TODO: figure out usage
	allowSeeks uint64

	// File number in the db
	// decide the sstable file name
	number uint64

	fileSize uint64

	// Smallest/largest key in the sstable
	smallest key.InternalKey
	largest  key.InternalKey
}

// TODO error handling
func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, meta.allowSeeks)
	binary.Write(w, binary.LittleEndian, meta.number)
	binary.Write(w, binary.LittleEndian, meta.fileSize)
	meta.smallest.EncodeTo(w)
	meta.largest.EncodeTo(w)
	return nil
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &meta.allowSeeks)
	binary.Read(r, binary.LittleEndian, &meta.number)
	binary.Read(r, binary.LittleEndian, &meta.fileSize)
	meta.smallest = key.InternalKey{}
	meta.smallest.DecodeFrom(r)
	meta.largest = key.InternalKey{}
	meta.largest.DecodeFrom(r)
	return nil
}

const (
	DefaultLevels            = 7
	L0_CompactionTrigger     = 4
	L0_SlowdownWritesTrigger = 8
)

// Version is a set of sstable files at a particular point in time.
// when flush memtable to sstable or compaction, a new version is created.
// version is stored as the manifest file.
//
// version enable us to read from a snapshot of the db in leveldb,
// which remains a todo in out project
type Version struct {
	// TODO table cache
	// tableCache     *TableCache

	dbName string

	// determine next newly created sstable file name
	nextFileNumber uint64

	// determine the seq when call memtable.Add()
	seq uint64

	// sstable is organized by level
	files [DefaultLevels][]FileMetaData

	// Per-level key at which the next compaction at that level should start.
	// Either an empty string, or a valid InternalKey.
	compactPointer [DefaultLevels][]byte
}

func New(dbName string) *Version {
	return &Version{
		dbName:         dbName,
		seq:            0,
		nextFileNumber: 1,
	}
}

// load a version from manifest file
func Load(dbName string, number uint64) (*Version, error) {
	fileName := manifestFileName(dbName, number)
	fd, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	v := New(dbName)
	err = v.DecodeFrom(fd)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *Version) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, v.nextFileNumber)
	binary.Write(w, binary.LittleEndian, v.seq)
	for level := range DefaultLevels {
		numFiles := len(v.files[level])
		binary.Write(w, binary.LittleEndian, int32(numFiles))
		for i := 0; i < numFiles; i++ {
			v.files[level][i].EncodeTo(w)
		}
	}
	return nil
}

func (v *Version) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)
	binary.Read(r, binary.LittleEndian, &v.seq)
	var numFiles int32
	for level := range DefaultLevels {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.files[level] = make([]FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			v.files[level][i].DecodeFrom(r)
		}
	}
	return nil
}

// add a sstable file to level
func (v *Version) addFile(level int, f FileMetaData) {
	logrus.Debugf("addFile, level:%d, fileNumber:%d, %s-%s", level, f.number, string(f.smallest.UserKey), string(f.largest.UserKey))

	// sstable in level 0 comes from memtable,and is not sorted globally
	if level == 0 {
		v.files[level] = append(v.files[level], f)
	} else {
		idx := sort.Search(len(v.files[level]), func(i int) bool {
			return key.InternalKeyCompareFunc(v.files[level][i].smallest, f.smallest) >= 0
		})
		v.files[level] = slices.Insert(v.files[level], idx, f)
	}
}

// when a memtable is full, write it to sstable at level 0
func (v *Version) writeLevel0Table(imm *memtable) error {
	var meta FileMetaData
	meta.number = v.nextFileNumber
	v.nextFileNumber++

	// convert memtable to sstable
	builder, err := newTableBuilder(sstableFileName(v.dbName, meta.number))
	if err != nil {
		return err
	}
	iter := imm.skl.Iterator()
	iter.SeekToFirst()
	if iter.Valid() {
		meta.smallest = iter.Key().(key.InternalKey)
		for ; iter.Valid(); iter.Next() {
			key := iter.Key().(key.InternalKey)
			value := iter.Value().([]byte)
			meta.largest = key
			w := new(bytes.Buffer)
			key.EncodeTo(w)
			builder.Add(w.Bytes(), value)
		}
		builder.Finish()
		meta.fileSize = builder.fileSize
	}

	v.addFile(0, meta)
	return nil
}

func (v *Version) pickLevel() int {
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
	for level := 0; level < DefaultLevels-1; level++ {
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
	logrus.Debugf("pick level %d", compactionLevel)
	return compactionLevel
}

// minor compact
func (v *Version) compact() {
	// compact level and level+1
	compactLevel := v.pickLevel()
	if compactLevel < 0 {
		return
	}
}

func totalFileSize(files []FileMetaData) uint64 {
	var sum uint64
	for i := 0; i < len(files); i++ {
		sum += files[i].fileSize
	}
	return sum
}
func maxBytesForLevel(level int) float64 {
	// Note: the result for level zero is not really used since we set
	// the level-0 compaction threshold based on number of files.

	// Result for both level-0 and level-1
	result := 10. * 1048576.0
	for level > 1 {
		result *= 10
		level--
	}
	return result
}
