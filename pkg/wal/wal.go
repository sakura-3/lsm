package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
)

const (
	initialSegmentID = 1
)

// TODO 实现 截取功能
type WAL struct {
	sync.RWMutex

	activeSegment *segment
	segments      map[SegmentID]*segment
	option        Option
}

type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func Open(option Option) (*WAL, error) {
	wal := &WAL{
		option:   option,
		segments: make(map[SegmentID]*segment),
	}

	if err := os.MkdirAll(option.Dir, 0777); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(option.Dir)
	if err != nil {
		return nil, err
	}

	segmentIDs := make([]int, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id uint32
		if _, err := fmt.Sscanf(entry.Name(), fileNameFormat, &id); err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, int(id))
	}

	if len(segmentIDs) == 0 {
		seg, err := openSegment(option.Dir, initialSegmentID, true)
		if err != nil {
			return nil, err
		}
		seg.active = true
		wal.activeSegment = seg
		wal.segments[initialSegmentID] = seg
		return wal, nil
	}

	sort.Ints(segmentIDs)
	for _, id := range segmentIDs {
		seg, err := openSegment(option.Dir, SegmentID(id), false)
		if err != nil {
			return nil, err
		}
		wal.segments[SegmentID(id)] = seg
	}
	wal.activeSegment = wal.segments[SegmentID(segmentIDs[len(segmentIDs)-1])]
	wal.activeSegment.active = true
	return wal, nil
}

// start 为 nil 时从最开始读,否则从第一个 >= start 的位置开始读
// 若不存在 >= start 的位置,移动至末尾
func (w *WAL) NewReaderWithStart(start *ChunkPosition) (*Reader, error) {
	w.RLock()
	defer w.RUnlock()

	readers := make([]*segmentReader, 0, len(w.segments))
	for _, seg := range w.segments {
		reader := seg.NewReader()
		readers = append(readers, reader)
	}
	sort.Slice(readers, func(i, j int) bool {
		return readers[i].segment.id < readers[j].segment.id
	})

	if start == nil {
		start = &ChunkPosition{
			SegmentID:   readers[0].segment.id,
			BlockN:      0,
			BlockOffset: 0,
		}
	}

	var currentReaderIndex int
	for i, reader := range readers {
		if reader.segment.id >= start.SegmentID {
			currentReaderIndex = i
			break
		}
	}
	logrus.Debugf("start: %+v, currentReaderIndex: %d", start, currentReaderIndex)
	// 末尾
	if currentReaderIndex == len(readers) {
		currentReaderIndex = len(readers) - 1
		sr := readers[currentReaderIndex]
		start.SegmentID = sr.segment.id
		start.BlockN = sr.segment.currentBlockN
		start.BlockOffset = sr.segment.currentBlockSize
	}
	// 对应的 start 可能已被 compact, 移动至所有记录的开头
	if readers[currentReaderIndex].segment.id > start.SegmentID {
		start.SegmentID = readers[currentReaderIndex].segment.id
		start.BlockN = 0
		start.BlockOffset = 0
	}

	currentReader := readers[currentReaderIndex]
	startOffset := uint64(start.BlockN)*blockSize + uint64(start.BlockOffset)
	currentOffset := uint64(0)
	logrus.Debugf("startOffset: %d, currentOffset: %d", startOffset, currentOffset)
	for currentOffset < startOffset {
		_, chunk, err := currentReader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		currentOffset += uint64(chunk.Size)
	}
	logrus.Debugf("currentReader:{id:%d, blockN:%d, blockOffset:%d}", currentReader.segment.id, currentReader.blockN, currentReader.blockOffset)
	return &Reader{
		segmentReaders: readers,
		currentReader:  currentReaderIndex,
	}, nil
}

func (w *WAL) Write(data []byte) (*ChunkPosition, error) {
	w.Lock()
	defer w.Unlock()

	if w.isFull(uint64(len(data))) {
		if err := w.cycle(); err != nil {
			return nil, err
		}
	}

	chunkPos, err := w.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	// sync 失败时可以根据该记录进行回滚
	logrus.Debugf("write chunk position: %+v", chunkPos)
	if w.option.Sync {
		if err := w.activeSegment.Sync(); err != nil {
			logrus.Debugf("sync failed, truncate to %+v", chunkPos)
			if err1 := w.activeSegment.Truncate(chunkPos.BlockN, uint64(chunkPos.BlockOffset)); err1 != nil {
				return nil, err1
			}
			return nil, err
		}
	}

	return chunkPos, nil
}

func (w *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	w.RLock()
	defer w.RUnlock()
	seg, ok := w.segments[pos.SegmentID]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", pos.SegmentID)
	}
	return seg.Read(pos.BlockN, uint64(pos.BlockOffset))
}

func (w *WAL) Sync() error {
	w.Lock()
	defer w.Unlock()
	return w.activeSegment.Sync()
}

func (w *WAL) Close() error {
	w.Lock()
	defer w.Unlock()

	for _, seg := range w.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	w.segments = nil
	w.activeSegment = nil
	return nil
}

// active segment 满了，创建新的 segment
func (w *WAL) cycle() error {
	if err := w.activeSegment.Sync(); err != nil {
		return err
	}
	seg, err := openSegment(w.option.Dir, w.activeSegment.id+1, true)
	if err != nil {
		return err
	}

	seg.active = true
	w.activeSegment.active = false
	w.activeSegment = seg
	w.segments[seg.id] = seg
	return nil
}

// 当前 segment 是否能写入 data
func (w *WAL) isFull(dataLen uint64) bool {
	t := dataLen
	// 最大 padding
	t += chunkHeaderSize
	// 写入的 header
	t += (dataLen + blockSize - 1) / blockSize * chunkHeaderSize

	return w.activeSegment.Size()+t > w.option.SegmentSize
}

func (w *WAL) Empty() bool {
	w.RLock()
	defer w.RUnlock()

	return len(w.segments) == 1 && w.activeSegment.Size() == 0
}

func (w *WAL) Delete() error {
	w.Lock()
	defer w.Unlock()

	for _, seg := range w.segments {
		if err := seg.Remove(); err != nil {
			return err
		}
	}
	w.segments = nil
	w.activeSegment = nil
	return nil
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	data, pos, err := r.segmentReaders[r.currentReader].Next()
	if errors.Is(err, io.EOF) {
		r.currentReader++
		return r.Next()
	}
	return data, pos, err
}

func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	cr := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentID:   cr.segment.id,
		BlockN:      cr.blockN,
		BlockOffset: cr.blockOffset,
	}
}
