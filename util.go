package lsm

import "fmt"

func manifestFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/MANIFEST-%06d", dbname, number)
}

func fileName(dbname string, number uint64, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", dbname, number, suffix)
}

func sstableFileName(dbname string, number uint64) string {
	return fileName(dbname, number, "ldb")
}
