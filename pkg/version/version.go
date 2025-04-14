package version

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"lsm/internal/key"
	"lsm/internal/util"
	"lsm/pkg/memtable"
	"lsm/pkg/sstable"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
)

// represent a sstable file in the disk
type FileMetaData struct {
	// Seeks allowed until compaction in leveldb
	// TODO: figure out usage
	allowSeeks uint64

	// File number in the db
	// decide the sstable file name
	dbName string
	number uint64

	fileSize uint64

	// Smallest/largest key in the sstable
	// 对应 internalKey 的二进制表达
	smallest *key.InternalKey
	largest  *key.InternalKey
}

func (meta *FileMetaData) Size() int {
	return 8 + // allowSeeks
		4 + len(meta.dbName) + // dbName
		8 + // number
		8 + // fileSize
		4 + len(meta.smallest.EncodeTo()) + // smallest
		4 + len(meta.largest.EncodeTo()) // largest
}

// TODO error handling
func (meta *FileMetaData) EncodeTo(w io.Writer) {
	binary.Write(w, binary.LittleEndian, meta.allowSeeks)
	w.Write(util.LenPrefixSlice([]byte(meta.dbName)))
	binary.Write(w, binary.LittleEndian, meta.number)
	binary.Write(w, binary.LittleEndian, meta.fileSize)
	w.Write(util.LenPrefixSlice(meta.smallest.EncodeTo()))
	w.Write(util.LenPrefixSlice(meta.largest.EncodeTo()))
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) {
	var (
		allowSeeks uint64
		lenPrefix  = make([]byte, 4)
		dbName     []byte
		number     uint64
		fileSize   uint64
		smallest   []byte
		largest    []byte
	)

	binary.Read(r, binary.LittleEndian, &allowSeeks)

	r.Read(lenPrefix)
	dbName = make([]byte, binary.LittleEndian.Uint32(lenPrefix))
	r.Read(dbName)

	binary.Read(r, binary.LittleEndian, &number)

	binary.Read(r, binary.LittleEndian, &fileSize)

	r.Read(lenPrefix)
	smallest = make([]byte, binary.LittleEndian.Uint32(lenPrefix))
	r.Read(smallest)

	r.Read(lenPrefix)
	largest = make([]byte, binary.LittleEndian.Uint32(lenPrefix))
	r.Read(largest)

	meta.allowSeeks = allowSeeks
	meta.dbName = string(dbName)
	meta.number = number
	meta.fileSize = fileSize
	meta.smallest.DecodeFrom(smallest)
	meta.largest.DecodeFrom(largest)
}

// load a sstable file from disk
func (meta *FileMetaData) Load() (*sstable.SSTable, error) {
	sstable, err := sstable.Open(util.SstableFileName(meta.dbName, meta.number))
	if err != nil {
		return nil, err
	}
	return sstable, nil
}

const (
	DefaultLevels = 7

	// level 0 文件数量达到多少开始 compaction
	L0_CompactionTrigger     = 4
	L0_SlowdownWritesTrigger = 8

	// sstable 文件大小, 1GB
	MaxSSTableFileSize = 1 << 30
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
	files [DefaultLevels][]*FileMetaData

	// Per-level key at which the next compaction at that level should start.
	// Either an empty string, or a valid InternalKey.
	compactPointer [DefaultLevels][]byte

	maxFileSize uint64
}

func New(dbName string) *Version {
	if err := os.MkdirAll(dbName, 0755); err != nil {
		panic(err)
	}
	return &Version{
		dbName:         dbName,
		seq:            0,
		nextFileNumber: 1,
		maxFileSize:    MaxSSTableFileSize,
	}
}

// load a version from manifest file
func Load(dbName string, number uint64) (*Version, error) {
	fileName := util.ManifestFileName(dbName, number)
	fd, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	v := New(dbName)
	err = v.decodeFrom(fd)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *Version) Save() (uint64, error) {
	tmp := v.nextFileNumber
	fileName := util.ManifestFileName(v.dbName, v.nextFileNumber)
	v.nextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return tmp, err
	}
	defer file.Close()
	v.encodeTo(file)
	return tmp, nil
}

func (v *Version) encodeTo(w io.Writer) error {
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

func (v *Version) decodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)
	binary.Read(r, binary.LittleEndian, &v.seq)
	var numFiles int32
	for level := range DefaultLevels {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.files[level] = make([]*FileMetaData, numFiles)
		for i := range int(numFiles) {
			v.files[level][i].DecodeFrom(r)
		}
	}
	return nil
}

// add a sstable file to level
func (v *Version) addFile(level int, f *FileMetaData) {
	logrus.Debugf("addFile, level:%d, fileNumber:%d, [%s,%s]", level, f.number, string(f.smallest.UserKey), string(f.largest.UserKey))

	// sstable in level 0 comes from memtable,and is not sorted globally
	if level == 0 {
		v.files[level] = append(v.files[level], f)
	} else {
		idx := sort.Search(len(v.files[level]), func(i int) bool {
			return key.InternalKeyCompareFunc(v.files[level][i].smallest.EncodeTo(), f.smallest.EncodeTo()) >= 0
		})
		v.files[level] = slices.Insert(v.files[level], idx, f)
	}
}

// remove a sstable file from level
// f must in v.files[level]
func (v *Version) deleteFile(level int, f *FileMetaData) {
	v.files[level] = slices.DeleteFunc(v.files[level], func(f2 *FileMetaData) bool {
		return f2.number == f.number
	})
}

// when a memtable is full, write it to sstable at level 0
func (v *Version) WriteLevel0Table(imm *memtable.Memtable) error {
	meta := FileMetaData{
		dbName:   v.dbName,
		number:   v.nextFileNumber,
		fileSize: 0,
		smallest: new(key.InternalKey),
		largest:  new(key.InternalKey),
	}
	v.nextFileNumber++

	// convert memtable to sstable
	builder, err := sstable.NewTableBuilder(util.SstableFileName(v.dbName, meta.number))
	if err != nil {
		return err
	}
	iter := imm.Iterator()
	iter.SeekToFirst()
	if iter.Valid() {
		meta.smallest.DecodeFrom(iter.Key())
		for ; iter.Valid(); iter.Next() {
			key := iter.Key()
			meta.largest.DecodeFrom(key)
			builder.Add(key, nil)
		}
		if err := builder.Finish(); err != nil {
			return err
		}
		meta.fileSize = builder.FileSize()
	}

	v.addFile(0, &meta)
	return nil
}

func (v *Version) Get(userKey []byte, seq uint64) ([]byte, bool) {
	// 获取最新的 value
	lookupKey := key.NewLookupKey(userKey, seq)

	// level 0 不是全局有序，且存在重合,需要全局扫描
	for _, f := range v.files[0] {
		if bytes.Compare(userKey, f.smallest.UserKey) < 0 || bytes.Compare(userKey, f.largest.UserKey) > 0 {
			continue
		}

		sstable, err := f.Load()
		if err != nil {
			logrus.Errorf("load sstable %d error:%v", f.number, err)
			return nil, false
		}

		value, ok := sstable.Get(lookupKey)
		if ok {
			return value, true
		}
	}

	// 其它 level 内部是全局有序的,可以通过 二分查找 快速定位
	// 低 level 的数据比高 level 的数据更新,因此在低 level 中找到后就无需查找高 level
	for level := 1; level < DefaultLevels; level++ {
		if len(v.files[level]) == 0 {
			continue
		}

		// 二分查找
		idx := sort.Search(len(v.files[level]), func(i int) bool {
			return bytes.Compare(v.files[level][i].largest.UserKey, userKey) >= 0
		})

		if idx == len(v.files[level]) {
			continue
		}

		sstable, err := v.files[level][idx].Load()
		if err != nil {
			logrus.Errorf("load sstable %d error:%v", v.files[level][idx].number, err)
			return nil, false
		}

		value, ok := sstable.Get(lookupKey)
		if ok {
			return value, true
		}
	}

	return nil, false
}

func (v *Version) Debug() string {
	var sb strings.Builder

	sb.WriteString("\n")
	for level := range DefaultLevels {
		sb.WriteString(fmt.Sprintf("level %d: ", level))
		for i := range v.files[level] {
			sb.WriteString(fmt.Sprintf("%d ", v.files[level][i].number))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func (v *Version) NextSeq() uint64 {
	v.seq++
	return v.seq
}

func (v *Version) NumLevelFiles(l int) int {
	return len(v.files[l])
}

func (v *Version) Copy() *Version {
	var c Version

	c.nextFileNumber = v.nextFileNumber
	c.seq = v.seq
	for level := 0; level < DefaultLevels; level++ {
		c.files[level] = make([]*FileMetaData, len(v.files[level]))
		copy(c.files[level], v.files[level])
	}
	return &c
}

func totalFileSize(files []*FileMetaData) uint64 {
	var sum uint64
	for i := range files {
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
