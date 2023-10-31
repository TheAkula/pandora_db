package pandora_db

import "encoding/binary"

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// FreeList node structure
// | type | size | total | next |  pointers |
// |  2B  |  2B  |   8B  |  8B  | size * 8B |
type FreeList struct {
	head uint64
	
	get func(uint64) BNode
	new func(BNode) uint64
	use func(uint64, BNode)
}

func flnSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node.data[2:]))
}

func flnNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node.data[12:])
}

func flnPtr(node BNode, index int) uint64 {
	assert(index < flnSize(node))
	offset := FREE_LIST_HEADER + index * 8
	return binary.LittleEndian.Uint64(node.data[offset:])
}

func flnSetPtr(node BNode, index int, ptr uint64) {
	assert(index < flnSize(node))
	offset := FREE_LIST_HEADER + index * 8
	binary.LittleEndian.PutUint64(node.data[offset:], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[2:], size)
	binary.LittleEndian.PutUint64(node.data[12:], next)
}

func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[4:], total)
}

// number of items in the list
func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}

	node := fl.get(fl.head)

	return int(binary.LittleEndian.Uint64(node.data[4:]))
}
// get the nth pointer
func (fl *FreeList) Get(topn int) uint64 {
	assert(0 <= topn && topn < fl.Total())
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		assert(next != 0)
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node) - topn - 1)
}
// remove `popn` pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	assert(popn <= fl.Total())
	if popn == 0 && len(freed) == 0 {
		return
	}

	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse) * FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head)
		if popn >= flnSize(node) {
				popn -= flnSize(node)
		} else {
			remain := flnSize(node) - popn
			popn = 0
			
			for remain > 0 && len(reuse) * FREE_LIST_CAP < len(freed) + remain {
				remain--;
				reuse = append(reuse, flnPtr(node, remain))
			}

			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}

		total -= flnSize(node)
		fl.head = flnNext(node)		
	}
	assert(len(reuse) * FREE_LIST_CAP >= len(freed) || fl.head == 0)	

	flPush(fl, freed, reuse)

	flnSetTotal(fl.get(fl.head), uint64(total + len(freed)))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		node := BNode{make([]byte, BTREE_PAGE_SIZE)}

		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(node, uint16(size), fl.head)

		for i := 0; i < size; i++ {
			flnSetPtr(node, i, freed[i])
		}

		freed = freed[size:]

		if len(reuse) > 0 {
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, node)
		} else {
			fl.head = fl.new(node)
		}
	}
	assert(len(reuse) == 0)
}