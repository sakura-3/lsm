package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Size(t *testing.T) {
	dir, _ := os.MkdirTemp("./", "test_seg_size")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	b1 := []byte(strings.Repeat("X", blockSize+100))
	_, err = seg.Write(b1)
	assert.Nil(t, err)
	_, err = seg.Write(b1)
	assert.Nil(t, err)

	info, err := seg.fd.Stat()
	assert.Nil(t, err)

	assert.Equal(t, uint64(info.Size()), seg.Size())
}

func TestSegment_Write_Full_1(t *testing.T) {
	dir, _ := os.MkdirTemp("./", "test_seg_write_full_1")
	s, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = s.Remove()
		os.Remove(dir)
	}()

	// 1. FULL chunks
	val := []byte(strings.Repeat("X", 100))

	pos1, err := s.Write(val)
	assert.Nil(t, err)
	pos2, err := s.Write(val)
	assert.Nil(t, err)

	val1, err := s.Read(pos1.BlockN, uint64(pos1.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	val2, err := s.Read(pos2.BlockN, uint64(pos2.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, val, val2)

	// 2. Write until a new block
	for i := 0; i < 100000; i++ {
		pos, err := s.Write(val)
		assert.Nil(t, err)

		res, err := s.Read(pos.BlockN, uint64(pos.BlockOffset))
		assert.Nil(t, err)
		assert.Equal(t, val, res)
	}
}

func TestSegment_Write_Full_2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_write_full_2")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	// 3. chunk full with a block
	val := []byte(strings.Repeat("X", blockSize-chunkHeaderSize))

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos1.BlockN, uint32(0))
	assert.Equal(t, pos1.BlockOffset, uint32(0))
	val1, err := seg.Read(pos1.BlockN, uint64(pos1.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	pos2, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos2.BlockN, uint32(1))
	assert.Equal(t, pos2.BlockOffset, uint32(0))
	val2, err := seg.Read(pos2.BlockN, uint64(pos2.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, val, val2)
}

func TestSegment_Write_Padding(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-padding")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 4. padding
	val := []byte(strings.Repeat("X", blockSize-chunkHeaderSize-3))

	_, err = seg.Write(val)
	assert.Nil(t, err)

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos1.BlockN, uint32(1))
	assert.Equal(t, pos1.BlockOffset, uint32(0))
	val1, err := seg.Read(pos1.BlockN, uint64(pos1.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, val, val1)
}

func TestSegment_Write_Not_Full(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_write_not_full")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	// chunkFirst - chunkLast
	b1 := []byte(strings.Repeat("X", blockSize+100))

	p1, err := seg.Write(b1)
	assert.Nil(t, err)
	v1, err := seg.Read(p1.BlockN, uint64(p1.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, b1, v1)

	p2, err := seg.Write(b1)
	assert.Nil(t, err)
	v2, err := seg.Read(p2.BlockN, uint64(p2.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, b1, v2)

	p3, err := seg.Write(b1)
	assert.Nil(t, err)
	v3, err := seg.Read(p3.BlockN, uint64(p3.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, b1, v3)

	// chunkFirst - chunkMiddle - chunkLast
	b2 := []byte(strings.Repeat("X", blockSize*3+100))
	p4, err := seg.Write(b2)
	assert.Nil(t, err)
	v4, err := seg.Read(p4.BlockN, uint64(p4.BlockOffset))
	assert.Nil(t, err)
	assert.Equal(t, b2, v4)
}

func TestSegment_Read_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_read_full")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	// FULL chunks
	b1 := []byte(strings.Repeat("X", blockSize+100))
	p1, err := seg.Write(b1)
	assert.Nil(t, err)
	p2, err := seg.Write(b1)
	assert.Nil(t, err)

	reader := seg.NewReader()
	v1, rpos1, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b1, v1)
	assert.Equal(t, p1, rpos1)

	v2, rpos2, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b1, v2)
	assert.Equal(t, p2, rpos2)

	v3, rpos3, err := reader.Next()
	assert.Nil(t, v3)
	assert.Nil(t, rpos3)
	assert.Equal(t, err, io.EOF)
}

func TestSegment_Read_Padding(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_read_padding")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	b := []byte(strings.Repeat("X", blockSize-chunkHeaderSize-7))
	p1, err := seg.Write(b)
	assert.Nil(t, err)
	p2, err := seg.Write(b)
	assert.Nil(t, err)

	reader := seg.NewReader()
	v1, rpos1, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b, v1)
	assert.Equal(t, p1, rpos1)

	v2, rpos2, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b, v2)
	assert.Equal(t, p2, rpos2)

	v3, rpos3, err := reader.Next()
	assert.Nil(t, v3)
	assert.Nil(t, rpos3)
	assert.Equal(t, err, io.EOF)
}

func TestSegment_Read_Not_Full(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_read_not_full")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	b1 := []byte(strings.Repeat("X", blockSize+100))
	p1, err := seg.Write(b1)
	assert.Nil(t, err)
	p2, err := seg.Write(b1)
	assert.Nil(t, err)

	b2 := []byte(strings.Repeat("X", blockSize*3+100))
	p3, err := seg.Write(b2)
	assert.Nil(t, err)
	p4, err := seg.Write(b2)
	assert.Nil(t, err)

	reader := seg.NewReader()
	v, rpos1, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b1, v)
	assert.Equal(t, p1, rpos1)

	v, rpos2, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b1, v)
	assert.Equal(t, p2, rpos2)

	v, rpos3, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b2, v)
	assert.Equal(t, p3, rpos3)

	v, rpos4, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, b2, v)
	assert.Equal(t, p4, rpos4)

	v, rpos5, err := reader.Next()
	assert.Nil(t, v)
	assert.Nil(t, rpos5)
	assert.Equal(t, err, io.EOF)
}

func TestSegment_Read_ManyChunks_Full(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_read_manychunks_full")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	N := 100_000
	postions := make([]*ChunkPosition, N)
	b := []byte(strings.Repeat("X", 512))
	for i := range N {
		pos, err := seg.Write(b)
		assert.Nil(t, err)
		postions[i] = pos
	}

	reader := seg.NewReader()
	i := 0
	for {
		v, rpos, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, b, v)
		assert.Equal(t, postions[i], rpos)

		i++
	}
}

func TestSegment_Read_ManyChunks_NotFull(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_seg_read_many_chunks_not_full")
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	N := 100_00
	postions := make([]*ChunkPosition, N)
	b := []byte(strings.Repeat("X", blockSize*3+10))
	for i := range N {
		pos, err := seg.Write(b)
		assert.Nil(t, err)
		postions[i] = pos
	}

	reader := seg.NewReader()
	i := 0
	for {
		v, rpos, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, b, v)
		assert.Equal(t, postions[i], rpos)

		i++
	}
	assert.Equal(t, N, i)
}

func TestSegment_Write_LargeSize(t *testing.T) {
	t.Run("Block-10000", func(t *testing.T) {
		testSegmentReaderLargeSize(t, blockSize-chunkHeaderSize, 10000)
	})
	t.Run("32*Block-1000", func(t *testing.T) {
		testSegmentReaderLargeSize(t, 32*blockSize, 1000)
	})
	t.Run("64*Block-100", func(t *testing.T) {
		testSegmentReaderLargeSize(t, 64*blockSize, 100)
	})
}

func testSegmentReaderLargeSize(t *testing.T, size int, count int) {
	dir, _ := os.MkdirTemp("", fmt.Sprintf("seg-test-reader-ManyChunks_large_size_%d_%d", size, count))
	seg, err := openSegment(dir, 1, true)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
		os.Remove(dir)
	}()

	postions := make([]*ChunkPosition, count)
	b := []byte(strings.Repeat("W", size))
	for i := range count {
		pos, err := seg.Write(b)
		assert.Nil(t, err)
		postions[i] = pos
	}

	reader := seg.NewReader()
	i := 0
	for {
		v, rpos, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, b, v)
		assert.Equal(t, postions[i], rpos)

		i++
	}
	assert.Equal(t, count, i)
}
