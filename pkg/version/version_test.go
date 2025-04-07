package version

import (
	"fmt"
	"lsm/internal/key"
	"lsm/pkg/sstable"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
