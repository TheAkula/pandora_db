package pandora_db

import (
	"bytes"
	"encoding/binary"
)

/* BNode stucture
 * [header = [node_type = 2B] [keys_size = 2B]] [pointers = keys_size * 8B] [offsets = keys_size * 2B] [key_value pairs]
 */
type BNode struct {
	data []byte
}

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(index uint16) uint64 {
	assert(index < node.nkeys())
	offset := HEADER + index * 8;
	return binary.LittleEndian.Uint64(node.data[offset:])
}

func (node BNode) setPtr(index uint16, value uint64) {
	assert(index < node.nkeys())
	offset := HEADER + index * 8
	binary.LittleEndian.PutUint64(node.data[offset:], value)
}

// offset
func offsetPos(node BNode, index uint16) uint16 {
	assert(1 <= index && index <= node.nkeys())
	return HEADER + node.nkeys() * 8 + 2 * (index - 1)
}

func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node.data[offsetPos(node, index):])
}

func (node BNode) setOffset(index uint16, value uint16) {	
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, index):], value)
}

// key-values
func (node BNode) kvPos(index uint16) uint16 {
	assert(index <= node.nkeys())
	return HEADER + node.nkeys() * 8 + node.nkeys() * 2 + node.getOffset(index)
}

func (node BNode) getKey(index uint16) []byte {
	assert(index <= node.nkeys())
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos + 4:][:klen]
}

func (node BNode) getValue(index uint16) []byte {
	assert(index <= node.nkeys())
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16(node.data[pos + 0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos + 2:])
	return node.data[pos + 4 + klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// look up
func nodeLookupLE(node BNode, key []byte) uint16 {
	nk := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nk; i++ {
		cmp := bytes.Compare(key, node.getKey(i))
		
		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}
	return found
}

func nodeAppendRange(new BNode, old BNode, dst uint16, src uint16, n uint16) {	
	assert(src + n <= old.nkeys())
	assert(dst + n <= new.nkeys())

	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dst + i, old.getPtr(src + i))
	}

	// offsets
	dstBegin := new.getOffset(dst)
	srcBegin := old.getOffset(src)
	for i := uint16(1); i <= n; i++ {
		new.setOffset(dst + i, dstBegin + old.getOffset(src + i) - srcBegin)
	}

	// kv
	begin := old.kvPos(src)
	end := old.kvPos(src + n)
	copy(new.data[new.kvPos(dst):], old.data[begin:end])
}

func nodeAppendKV(node BNode, index uint16, ptr uint64, key []byte, value []byte) {	
	node.setPtr(index, ptr)
	
	pos := node.kvPos(index)	
	binary.LittleEndian.PutUint16(node.data[pos + 0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node.data[pos + 2:], uint16(len(value)))
	copy(node.data[pos + 4:], key)
	copy(node.data[pos + 4 + uint16(len(key)):], value)

	node.setOffset(index + 1, node.getOffset(index) + 4 + uint16(len(key) + len(value)))
}