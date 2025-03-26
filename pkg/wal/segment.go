package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

type ChunkType byte
type SegmentID uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var stmp = [...]string{
	"full",
	"first",
	"middle",
	"last",
}

func (ct ChunkType) String() string {
	return stmp[ct]
}

var (
	ErrClosed      = errors.New("segment is closed")
	ErrInvalidCRC  = errors.New("invalid crc")
	ErrChunkTooBig = errors.New("chunk is too big")
	ErrInactive    = errors.New("inactive segment can't write")
)

const (
	// checksum + length + chunktype
	// 4 + 2 + 1
	chunkHeaderSize = 7

	maxChunkSize = math.MaxUint16

	blockSize = 32 * KB

	fileModePerm = 0644

	fileNameFormat = "%016d.seg"
)

type segment struct {
	id SegmentID
	fd *os.File

	// currentBlockN * blockSize + currentBlockSize 即为待写入的位置
	currentBlockN    uint32
	currentBlockSize uint32

	// 避免每次写入 chunk 时都要重新分配 header 空间
	header []byte

	closed bool

	// 是否正在写入
	active bool
}

// BlockN * blockSize + BlockOffset 即为该 chunk 在文件中的偏移量
type ChunkPosition struct {
	SegmentID   SegmentID
	BlockN      uint32
	BlockOffset uint32
	Size        uint32
}

func segmentFileName(id SegmentID) string {
	return fmt.Sprintf(fileNameFormat, id)
}

func openSegment(dir string, id SegmentID, active bool) (*segment, error) {
	filename := filepath.Join(dir, segmentFileName(id))
	fd, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek file %s end failed: %w", filename, err)
	}

	return &segment{
		id:               id,
		fd:               fd,
		header:           make([]byte, chunkHeaderSize),
		currentBlockN:    uint32(offset / blockSize),
		currentBlockSize: uint32(offset % blockSize),
		active:           active,
	}, nil
}

func (s *segment) appendToBuffer(chunkBuffer *bytes.Buffer, data []byte, chunkType ChunkType) error {
	if len(data) > maxChunkSize {
		return ErrChunkTooBig
	}

	binary.BigEndian.PutUint16(s.header[4:6], uint16(len(data)))
	s.header[6] = byte(chunkType)

	checksum := crc32.ChecksumIEEE(s.header[4:])
	checksum = crc32.Update(checksum, crc32.IEEETable, data)
	binary.BigEndian.PutUint32(s.header[:4], checksum)

	chunkBuffer.Write(s.header)
	chunkBuffer.Write(data)

	return nil
}

// 将 data 写入 chunkBuffer,并更新 s 的 currentBlockN 等字段
// 后续会将 chunkBuffer 写入到 s.fd 中
func (s *segment) writeToBuffer(data []byte, chunkBuffer *bytes.Buffer) (*ChunkPosition, error) {
	if s.closed {
		return nil, ErrClosed
	}

	if !s.active {
		return nil, ErrInactive
	}

	padding := uint32(0)
	startBufferLen := chunkBuffer.Len()

	logrus.Debugf("try to write len(data)=%d to buffer[%d:]", len(data), startBufferLen)

	// 当前 block 不够写入 chunk header,进行填充
	if s.currentBlockSize+chunkHeaderSize >= blockSize {

		p := blockSize - s.currentBlockSize
		padding += p
		chunkBuffer.Write(make([]byte, p))
		logrus.Debugf("current block is full, padding %d bytes", p)

		s.currentBlockN++
		s.currentBlockSize = 0
	}

	pos := &ChunkPosition{
		SegmentID:   s.id,
		BlockN:      s.currentBlockN,
		BlockOffset: s.currentBlockSize,
	}

	dataSize := uint32(len(data))
	if s.currentBlockSize+chunkHeaderSize+dataSize <= blockSize {
		s.appendToBuffer(chunkBuffer, data, ChunkTypeFull)
		pos.Size = chunkHeaderSize + dataSize

		logrus.Debugf("write full chunk to buffer")
	} else {
		var (
			start, end       = uint32(0), uint32(0) // 每次写入 data[start:end]
			blockN           = uint32(0)            // 记录当前写入的 block 数量
			currentBlockSize = s.currentBlockSize
		)
		for {
			leftBlockSize := blockSize - currentBlockSize - chunkHeaderSize
			end = min(dataSize, start+leftBlockSize)

			var chunkType ChunkType
			switch {
			case start == 0:
				chunkType = ChunkTypeFirst
			case end == dataSize:
				chunkType = ChunkTypeLast
			default:
				chunkType = ChunkTypeMiddle
			}

			s.appendToBuffer(chunkBuffer, data[start:end], chunkType)

			logrus.Debugf("write data[%d:%d] to buffer[blockN=%d, currentBlockSize=%d], chunkType=%s, ", start, end, blockN, currentBlockSize, chunkType)

			blockN++
			currentBlockSize = (currentBlockSize + chunkHeaderSize + end - start) % blockSize
			start = end

			if chunkType == ChunkTypeLast {
				break
			}
		}

		pos.Size = chunkHeaderSize*blockN + dataSize
	}

	// 检测 预期写入长度 与 实际写入长度
	if pos.Size+padding != uint32(chunkBuffer.Len()-startBufferLen) {
		return nil, fmt.Errorf("write to buffer failed: expected %d written, got %d witten", pos.Size+padding, uint32(chunkBuffer.Len()-startBufferLen))
	}

	s.currentBlockSize += pos.Size
	s.currentBlockN += s.currentBlockSize / blockSize
	s.currentBlockSize %= blockSize
	return pos, nil
}

func (s *segment) writeBufferToFile(chunkBuffer *bytes.Buffer) error {
	logrus.Debugf("try to write buffer to file,len(buffer)=%d", chunkBuffer.Len())
	if _, err := s.fd.Write(chunkBuffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if s.closed {
		return nil, ErrClosed
	}

	if !s.active {
		return nil, ErrInactive
	}

	// 写入失败时 rollback
	originBlockN := s.currentBlockN
	originBlockSize := s.currentBlockSize

	chunkBuffer := getBuffer()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			s.currentBlockN = originBlockN
			s.currentBlockSize = originBlockSize
		}
		putBuffer(chunkBuffer)
	}()

	pos, err = s.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}
	if err = s.writeBufferToFile(chunkBuffer); err != nil {
		return
	}

	return
}

func (s *segment) Read(blockN uint32, blockOffset uint64) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}

	var (
		result             []byte
		block              = getBlock()
		currentBlockN      = blockN
		currentBlockOffset = blockOffset
	)
	defer putBlock(block)

	for {
		offset := uint64(currentBlockN * blockSize)
		size := uint64(blockSize)
		if offset+size > uint64(s.Size()) {
			size = uint64(s.Size()) - offset
		}
		if size <= 0 {
			return nil, io.EOF
		}

		if _, err := s.fd.ReadAt(block[:size], int64(offset)); err != nil {
			return nil, err
		}

		data, _, end, err := readBlock(block, currentBlockOffset)
		if err != nil {
			return nil, err
		}
		result = append(result, data...)

		if end {
			break
		}

		currentBlockN++
		currentBlockOffset = 0
	}

	return result, nil
}

type segmentReader struct {
	segment     *segment
	blockN      uint32
	blockOffset uint32
}

func (s *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     s,
		blockN:      0,
		blockOffset: 0,
	}
}

// 从 offset 处尝试读取一个完整的 chunk, 直到 block 末尾
// 返回读取的 data(不包括 header), size(包括 header ), 如果 chunk 未结束,  返回 false
func readBlock(block []byte, offset uint64) ([]byte, uint64, bool, error) {
	size := uint64(0)
	data := make([]byte, 0, len(block)-int(offset))

	for {
		if offset+chunkHeaderSize > uint64(len(block)) {
			return data, size, false, nil
		}

		header := block[offset : offset+chunkHeaderSize]
		savedChecksum := binary.BigEndian.Uint32(header[:4])
		length := binary.BigEndian.Uint16(header[4:6])
		chunkType := ChunkType(header[6])

		checksumEnd := offset + chunkHeaderSize + uint64(length)
		checksum := crc32.ChecksumIEEE(block[offset+4 : checksumEnd])
		logrus.Debugf("block[%d:%d] -> chunk{checksum=%x, length=%d, chunkType=%s}", offset, checksumEnd, savedChecksum, length, chunkType)
		if checksum != savedChecksum {
			logrus.Debugf("checksum failed: saved=%x, calculated=%x", savedChecksum, checksum)
			return nil, 0, false, ErrInvalidCRC
		}

		size += chunkHeaderSize + uint64(length)
		data = append(data, block[offset+chunkHeaderSize:checksumEnd]...)

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			return data, size, true, nil
		}
		offset += chunkHeaderSize + uint64(length)
	}

	panic("unreachable")
}

// Next 返回当前 chunk 的 data 和 对应的 chunk position
// 并将位置移动到下一个 chunk 的起始位置.当前 segment 读取结束后，返回 io.EOF
// TODO: 当多个 chunk 位于同一个 block 内时,读取每个 chunk 时都需要重新读取 block
func (sr *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if sr.segment.closed {
		return nil, nil, ErrClosed
	}

	var (
		result []byte
		block  = getBlock()
		chunk  = &ChunkPosition{
			SegmentID:   sr.segment.id,
			BlockN:      sr.blockN,
			BlockOffset: sr.blockOffset,
			Size:        0,
		}
		currentBlockN      = sr.blockN
		currentBlockOffset = sr.blockOffset
	)
	defer putBlock(block)

	logrus.Debugf("try to read chunk from segment[%d], blockN=%d, blockOffset=%d", sr.segment.id, currentBlockN, currentBlockOffset)
	for {
		offset := int64(currentBlockN * blockSize)
		size := int64(blockSize)
		if offset+size > int64(sr.segment.Size()) {
			size = int64(sr.segment.Size()) - offset
		}

		if size <= int64(currentBlockOffset) {
			return nil, nil, io.EOF
		}

		logrus.Debugf("file[%d:%d] -> block[:%d]", offset, offset+size, size)
		if _, err := sr.segment.fd.ReadAt(block[:size], int64(offset)); err != nil {
			return nil, nil, err
		}

		data, totSize, end, err := readBlock(block, uint64(currentBlockOffset))
		logrus.Debugf("len(data）=%d, totSize=%d, end=%t, err=%v", len(data), totSize, end, err)
		if err != nil {
			return nil, nil, err
		}
		chunk.Size += uint32(totSize)
		currentBlockOffset += uint32(totSize)
		result = append(result, data...)

		if end {
			break
		}

		// chunk 跨 block
		currentBlockN++
		currentBlockOffset = 0
	}

	// 校验
	if chunk.Size != uint32(currentBlockN*blockSize+currentBlockOffset)-uint32(chunk.BlockN*blockSize+chunk.BlockOffset) {
		panic("read size not match")
	}

	// 读取结束后, reader 移动至下一个 chunk 的起始位置
	// 需要考虑 padding
	if currentBlockOffset+chunkHeaderSize >= blockSize && uint64(currentBlockN*blockSize+currentBlockOffset) < sr.segment.Size() {
		logrus.Debugf("padding found, move to next chunk")
		currentBlockN++
		currentBlockOffset = 0
	}

	sr.blockN = currentBlockN
	sr.blockOffset = currentBlockOffset

	return result, chunk, nil
}

func (s *segment) Sync() error {
	if s.closed {
		return ErrClosed
	}
	return s.fd.Sync()
}

func (s *segment) Size() uint64 {
	return uint64(s.currentBlockN)*blockSize + uint64(s.currentBlockSize)
}

func (s *segment) Close() error {
	if s.closed {
		return nil
	}
	if err := s.Sync(); err != nil {
		return err
	}
	s.closed = true
	return s.fd.Close()
}

func (s *segment) Remove() error {
	if !s.closed {
		s.closed = true
		if err := s.fd.Close(); err != nil {
			return err
		}
	}

	return os.Remove(s.fd.Name())
}

// 截断 blockN * blockSize + blockOffset 之后的内容
// 当 wal.Write 中调用 s.Sync 失败时,写入的记录是不可靠的,需要进行 rollback
func (s *segment) Truncate(blockN uint32, blockOffset uint64) error {
	if s.closed {
		return ErrClosed
	}

	if !s.active {
		return ErrInactive
	}

	logrus.Debugf("truncate segment[%d], blockN=%d, blockOffset=%d", s.id, blockN, blockOffset)
	offset := uint64(blockN)*blockSize + blockOffset
	if uint64(blockN)*blockSize+blockOffset > s.Size() {
		return fmt.Errorf("invalid blockN or blockOffset")
	}

	if err := s.fd.Truncate(int64(offset)); err != nil {
		return err
	}
	s.fd.Seek(int64(offset), io.SeekStart)
	s.currentBlockN = blockN
	s.currentBlockSize = uint32(blockOffset)
	return nil
}
