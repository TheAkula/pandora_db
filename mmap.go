package pandora_db

import (
	"fmt"
	"os"
	"syscall"
)

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()

	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size() % BTREE_PAGE_SIZE != 0 {
		return 0, nil, fmt.Errorf("File size is not multiple of page size")
	}

	mmapSize := 64 << 20
	assert(mmapSize % BTREE_PAGE_SIZE == 0)
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED,
	)

	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages * BTREE_PAGE_SIZE {
		// no need to extend
		return nil
	}

	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}