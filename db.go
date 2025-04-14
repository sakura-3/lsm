package lsm

import (
	"fmt"
	"lsm/internal/key"
	"lsm/internal/util"
	"lsm/pkg/memtable"
	"lsm/pkg/version"
	"os"
	"strconv"
	"sync"
	"time"
)

type Db struct {
	name                  string
	mu                    sync.Mutex
	cond                  *sync.Cond
	mem                   *memtable.Memtable
	imm                   *memtable.Memtable
	current               *version.Version
	bgCompactionScheduled bool
}

const (
	// 1KB
	DefaultMemTableSize = 1024
)

func Open(dbName string) *Db {
	var db Db
	db.name = dbName
	db.mem = memtable.NewMemtable(DefaultMemTableSize)
	db.imm = nil
	db.bgCompactionScheduled = false
	db.cond = sync.NewCond(&db.mu)
	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := version.Load(dbName, num)
		if err != nil {
			return nil
		}
		db.current = v
	} else {
		db.current = version.New(dbName)
	}

	return &db
}

func (db *Db) Close() {
	db.mu.Lock()
	for db.bgCompactionScheduled {
		db.cond.Wait()
	}
	db.mu.Unlock()
}

func (db *Db) Put(userKey, userValue []byte) error {
	// May temporarily unlock and wait.
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// todo : add wal

	db.mem.Add(seq, key.KTypeValue, userKey, userValue)
	return nil
}

func (db *Db) Get(userKey []byte, seq uint64) ([]byte, bool) {
	db.mu.Lock()
	mem := db.mem
	imm := db.imm
	current := db.current
	db.mu.Unlock()
	value, ok := mem.Get(userKey, seq)
	if ok {
		return value, ok
	}

	if imm != nil {
		value, ok := imm.Get(userKey, seq)
		if ok {
			return value, ok
		}
	}

	value, ok = current.Get(userKey, seq)
	return value, ok
}

func (db *Db) Delete(userKey []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	db.mem.Add(seq, key.KTypeDeletion, userKey, nil)
	return nil
}

func (db *Db) makeRoomForWrite() (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for true {
		if db.current.NumLevelFiles(0) >= version.L0_SlowdownWritesTrigger {
			db.mu.Unlock()
			time.Sleep(time.Duration(1000) * time.Microsecond)
			db.mu.Lock()
		} else if db.mem.Full() {
			return db.current.NextSeq(), nil
		} else if db.imm != nil {
			//  Current memtable full; waiting
			db.cond.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			// todo : switch log
			db.imm = db.mem
			db.mem = memtable.NewMemtable(DefaultMemTableSize)
			db.maybeScheduleCompaction()
		}
	}

	return db.current.NextSeq(), nil
}

// from dbname/CURRENT read current file number of version
// if dbname/CURRENT not exist, return 0, represent a new db
// else db should load version from dbname/MANIFEST-[number]
func (db *Db) ReadCurrentFile() uint64 {
	content, err := os.ReadFile(util.CurrentFileName(db.name))
	if err == nil {
		return 0
	}
	num, err := strconv.Atoi(string(content))
	if err != nil {
		return 0
	}
	return uint64(num)
}

func (db *Db) SetCurrentFile(descriptorNumber uint64) {
	tmp := util.TempFileName(db.name, descriptorNumber)
	os.WriteFile(tmp, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(tmp, util.CurrentFileName(db.name))
}

func (db *Db) maybeScheduleCompaction() {
	if db.bgCompactionScheduled {
		return
	}
	db.bgCompactionScheduled = true
	go db.backgroundCall()
}

func (db *Db) backgroundCall() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.backgroundCompaction()
	db.bgCompactionScheduled = false
	db.cond.Broadcast()
}

func (db *Db) backgroundCompaction() {
	imm := db.imm
	version := db.current.Copy()
	db.mu.Unlock()

	// minor compaction
	if imm != nil {
		version.WriteLevel0Table(imm)
	}
	// major compaction
	for version.Compact() {
		version.Debug()
	}
	descriptorNumber, _ := version.Save()
	db.SetCurrentFile(descriptorNumber)
	db.mu.Lock()
	db.imm = nil
	db.current = version
}
