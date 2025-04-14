package version

import (
	"fmt"
	"lsm/internal/key"
	"lsm/pkg/memtable"
	"lsm/pkg/sstable"
	"math"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func init() {
// 	logrus.SetLevel(logrus.DebugLevel)
// }

func TestMergeIteratorBasic(t *testing.T) {
	sbs := make([]*sstable.TableBuilder, 3)
	for i := range 3 {
		sbs[i], _ = sstable.NewTableBuilder(fmt.Sprintf("TestMergeIteratorBasic%d.sst", i))
	}
	defer func() {
		for i := range 3 {
			os.Remove(fmt.Sprintf("TestMergeIteratorBasic%d.sst", i))
		}
	}()

	const (
		userKeyFormat   = "userkey-%09d"
		userValueFormat = "uservalue-%09d"
		keyN            = 100_000
	)
	internalKeys := make([]key.InternalKey, keyN)
	for i := range keyN {
		internalKeys[i] = key.New(fmt.Appendf(nil, userKeyFormat, i), fmt.Appendf(nil, userValueFormat, i), uint64(i), key.KTypeValue)
	}

	// insert by random table builder
	for _, k := range internalKeys {
		idx := rand.IntN(3)
		err := sbs[idx].Add(k.EncodeTo(), nil)
		assert.Nil(t, err)
	}

	// table builder finish
	for _, sb := range sbs {
		err := sb.Finish()
		assert.Nil(t, err)
	}

	// load sst file and create iter
	iters := make([]*sstable.SSTableIterator, 3)
	for i := range 3 {
		table, err := sstable.Open(fmt.Sprintf("TestMergeIteratorBasic%d.sst", i))
		assert.Nil(t, err)
		iters[i] = table.NewIterator()
	}

	// create merge iter
	mi := NewMergeIterator(iters)
	idx := 0
	for ; mi.Valid(); mi.Next() {
		assert.Equal(t, internalKeys[idx].EncodeTo(), mi.Key())
		idx++
	}
	assert.Equal(t, keyN, idx)
}

func TestWriteLevel0(t *testing.T) {
	const dbName = "TestWriteLevel0"
	// 1KB
	imm := memtable.NewMemtable(1024)
	v := New(dbName)
	defer os.RemoveAll(dbName)

	idx := 0
	deleted := map[int]bool{}
	for !imm.Full() {
		imm.Add(v.NextSeq(), key.KTypeValue, fmt.Appendf(nil, "userkey-%10d", idx), fmt.Appendf(nil, "uservalue-%10d", idx))
		if rand.Float64() < 0.25 {
			imm.Add(v.NextSeq(), key.KTypeDeletion, fmt.Appendf(nil, "userkey-%10d", idx), nil)
			deleted[idx] = true
		}
		idx++
	}
	t.Logf("insert %d keys,deleted=%v,seq=%d", idx, deleted, v.seq)

	err := v.WriteLevel0Table(imm)
	assert.Nil(t, err)

	t.Log(v.Debug())

	for i := range idx {
		userkey := fmt.Appendf(nil, "userkey-%10d", i)
		userValue, ok := v.Get(userkey, math.MaxUint64)
		if deleted[i] {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, fmt.Appendf(nil, "uservalue-%10d", i), userValue)
		}
	}
}

func TestCompact(t *testing.T) {
	// TODO
}
