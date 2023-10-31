package pandora_db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

const DB_SIG = "1616161616161616"

type KV struct {
	Path string

	fp *os.File
	tree BTree
	free FreeList

	mmap struct {
		file int // file size
		total int // mmap size
		chunks [][]byte // mmap data
	}	
	page struct {
		flushed uint64 // database size in number of pages		
		nfree int // number of pages taken from free list
		nappend int // number of pages to append
		updates map[uint64][]byte // newly allocated or deallocated pages 
	}
}

func (db *KV) pageGet(ptr uint64) BNode {	
	if page, ok := db.page.updates[ptr]; ok {
		assert(page != nil)

		return BNode{page}
	}
	return pageGetMapped(db, ptr)
}

func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk)) / BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset + BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("invalid ptr")
}

func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := uint64(0)

	if db.page.nfree < db.free.Total() {
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}

// db page structure:
// sig | root | pages used | free list |
// 16B |  8B  |     8B		 |     8B    |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		db.page.flushed = 1
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	free := binary.LittleEndian.Uint64(data[32:])
	
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("Bad database signature")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file / BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("Bad master page")
	}
	
	db.tree.root = root	
	db.page.flushed = used
	db.free.head = free
	return nil
}

func masterStore(db *KV) error {
	var data [40]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	binary.LittleEndian.PutUint64(data[32:], db.free.head)

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}

	return nil
}

func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		inc := filePages / 8
		if inc < 1 {
			inc = 1			
		}
		filePages += inc
	}

	fileSize := filePages * BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}

func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	db.fp = fp

	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}

	db.mmap.chunks = [][]byte{chunk}
	db.mmap.file = sz
	db.mmap.total = len(chunk)	

	db.tree.get = db.pageGet
	db.tree.del = db.pageDel
	db.tree.new = db.pageNew	

	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse

	db.page.updates = make(map[uint64][]byte)

	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = db.fp.Close()
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KV) error {
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	npages := int(db.page.flushed) + db.page.nappend
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).data, page)
		}
	}
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(db.page.nappend)
	db.page.updates = map[uint64][]byte{}

	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return err
	}
	return nil
}