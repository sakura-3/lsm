package util

import (
	"encoding/binary"
	"fmt"
)

func CurrentFileName(dbname string) string {
	return dbname + "/CURRENT"
}

func ManifestFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/MANIFEST-%06d", dbname, number)
}

func fileName(dbname string, number uint64, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", dbname, number, suffix)
}

func SstableFileName(dbname string, number uint64) string {
	return fileName(dbname, number, "ldb")
}

func TempFileName(dbname string, number uint64) string {
	return fileName(dbname, number, "dbtmp")
}

func LenPrefixSlice(data []byte) []byte {
	ret := make([]byte, len(data)+4)
	binary.LittleEndian.PutUint32(ret, uint32(len(data)))
	copy(ret[4:], data)
	return ret
}
