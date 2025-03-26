package wal

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func removeWAL(wal *WAL) {
	if wal != nil {
		_ = wal.Close()
		_ = os.RemoveAll(wal.option.Dir)
	}
}

func TestMain(m *testing.M) {
	testFile, err := os.OpenFile("test.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(testFile)
	os.Exit(m.Run())
}

func TestWAL_Write1(t *testing.T) {
	dir, _ := os.MkdirTemp(".", "test_wal_write1")
	wal, err := Open(Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
		Sync:        true,
	})
	assert.Nil(t, err)
	defer removeWAL(wal)

	pos1, err := wal.Write([]byte("hello1"))
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	pos2, err := wal.Write([]byte("hello2"))
	assert.Nil(t, err)
	assert.NotNil(t, pos2)
	pos3, err := wal.Write([]byte("hello3"))
	assert.Nil(t, err)
	assert.NotNil(t, pos3)

	val, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, "hello1", string(val))
	val, err = wal.Read(pos2)
	assert.Nil(t, err)
	assert.Equal(t, "hello2", string(val))
	val, err = wal.Read(pos3)
	assert.Nil(t, err)
	assert.Equal(t, "hello3", string(val))
}

func testWriteAndIterate(t *testing.T, wal *WAL, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		positions[i] = pos
	}

	var count int
	// iterates all the data
	reader, err := wal.NewReaderWithStart(nil)
	assert.Nil(t, err)
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, val, string(data))

		assert.Equal(t, positions[count], pos)

		count++
	}
	assert.Equal(t, size, count)
}

func TestWAL_Write_large(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write2")
	opts := Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer removeWAL(wal)

	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_Write_large2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write3")
	opts := Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer removeWAL(wal)

	testWriteAndIterate(t, wal, 2000, 32*1024*3+10)
}

func TestWAL_IsEmpty(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-is-empty")
	opts := Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer removeWAL(wal)

	assert.True(t, wal.Empty())
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.Empty())
}

func TestWAL_Reader(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader")
	opts := Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer removeWAL(wal)

	var size = 100000
	val := strings.Repeat("wal", 512)
	for i := 0; i < size; i++ {
		_, err := wal.Write([]byte(val))
		assert.Nil(t, err)
	}

	validate := func(walInner *WAL, size int) {
		var i = 0
		reader, err := walInner.NewReaderWithStart(nil)
		assert.Nil(t, err)
		for {
			chunk, position, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			assert.NotNil(t, chunk)
			assert.NotNil(t, position)
			assert.Equal(t, position.SegmentID, reader.CurrentChunkPosition().SegmentID)
			i++
		}
		assert.Equal(t, i, size)
	}

	validate(wal, size)
	err = wal.Close()
	assert.Nil(t, err)

	wal2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	validate(wal2, size)
}

func TestWAL_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-delete")
	opts := Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.Empty())
	defer removeWAL(wal)

	err = wal.Delete()
	assert.Nil(t, err)

	wal, err = Open(opts)
	assert.Nil(t, err)
	assert.True(t, wal.Empty())
}

func TestWAL_ReaderWithStart(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader-with-start")
	opts := Option{
		Dir:         dir,
		SegmentSize: 32 * MB,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer removeWAL(wal)

	reader1, err := wal.NewReaderWithStart(&ChunkPosition{SegmentID: 0, BlockN: 0, BlockOffset: 100})
	assert.Nil(t, err)
	assert.Equal(t, reader1.CurrentChunkPosition(), &ChunkPosition{SegmentID: 1, BlockN: 0, BlockOffset: 0})
	_, _, err = reader1.Next()
	assert.Equal(t, err, io.EOF)

	count := 1000
	b := []byte(strings.Repeat("wal", 512))
	postions := make([]*ChunkPosition, count)
	for i := range count {
		postions[i], err = wal.Write(b)
		assert.Nil(t, err)
	}

	reader2, err := wal.NewReaderWithStart(nil)
	assert.Nil(t, err)
	assert.Equal(t, reader2.CurrentChunkPosition(), &ChunkPosition{SegmentID: 1, BlockN: 0, BlockOffset: 0})
	var i int
	for {
		data, pos, err := reader2.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, data, b)
		assert.Equal(t, pos, postions[i])
		i++
	}
	assert.Equal(t, i, count)
}
