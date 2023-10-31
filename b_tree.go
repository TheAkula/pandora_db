package pandora_db

import (
	"bytes"
)

type BTree struct {
	root uint64
	
	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	assert(len(key) <= BTREE_MAX_KEY_SIZE)

	root := tree.get(tree.root)

	val := treeGet(tree, root, key)
	
	return val, val != nil	
}

func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) != 0)
	assert(len(key) <= BTREE_MAX_KEY_SIZE)
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)		
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key []byte, val []byte) {
	assert(len(key) != 0)
	assert(len(key) <= BTREE_MAX_KEY_SIZE)
	assert(len(val) <= BTREE_MAX_VAL_SIZE)
	
	if tree.root == 0 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// dummy key
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	node = treeInsert(tree, node, key, val)
	nsplit, splited := nodeSplit3(node)
	if nsplit > 1 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splited[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splited[0])
	}
}

// tree get
func treeGet(tree *BTree, node BNode, key []byte) []byte {
	assert(len(key) <= BTREE_MAX_KEY_SIZE)

	index := nodeLookupLE(node, key)
	
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(index)) {
			return leafGet(node, index)
		}
		return nil
	case BNODE_NODE:
		return nodeGet(tree, node, index, key)
	default:
		panic("invalid node type")
	}		
}

// tree insert
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}

	index := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(index)) {
			leafUpdate(new, node, index, key, val)
		} else {
			leafInsert(new, node, index + 1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, index, key, val)
	default:
		panic("invalid node type")
	}
	return new
}

// tree delete
func treeDelete(tree *BTree, node BNode, key []byte) BNode {	
	index := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(index)) {
			new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

			leafDelete(new, node, index)
			return new
		}
		return BNode{}
	case BNODE_NODE:
		return nodeDelete(tree, node, index, key)
	default:
		panic("invalid node type")
	}	
}

// leaf get
func leafGet(node BNode, index uint16) []byte {
	return node.getValue(index)
}

// leaf insert
func leafInsert(new BNode, old BNode, index uint16, key []byte, value []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys() + 1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, value)
	nodeAppendRange(new, old, index + 1, index, old.nkeys() - index)
}

// leaf update
func leafUpdate(new BNode, old BNode, index uint16, key []byte, value []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, value)
	nodeAppendRange(new, old, index + 1, index + 1, old.nkeys() - index - 1)
}

// leaf delete
func leafDelete(new BNode, old BNode, index uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys() - 1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendRange(new, old, index, index + 1, old.nkeys() - (index + 1))
}

// node get
func nodeGet(tree *BTree, node BNode, index uint16, key []byte) []byte {
	kptr := node.getPtr(index)
	knode := tree.get(kptr)

	return treeGet(tree, knode, key)
}

// node insert
func nodeInsert(tree *BTree, new BNode, node BNode, index uint16, key []byte, value []byte) {
	kptr := node.getPtr(index)
	knode := tree.get(kptr)
	
	tree.del(kptr)

	knode = treeInsert(tree, knode, key, value)

	nsplit, splited := nodeSplit3(knode)

	nodeReplaceKidN(tree, new, node, index, splited[:nsplit]...)
}

// node delete
func nodeDelete(tree *BTree, node BNode, index uint16, key []byte) BNode {
	kptr := node.getPtr(index)
	updated := treeDelete(tree, tree.get(kptr), key)	
	if len(updated.data) == 0 {
		// not found
		return BNode{}
	}

	tree.del(kptr)

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	mergeDir, sibling := shouldMerge(tree, node, index, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		// delete sibling
		tree.del(node.getPtr(index - 1))
		nodeReplace2Kid(new, node, index - 1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(index + 1))
		nodeReplace2Kid(new, node, index, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		if updated.nkeys() == 0 {
			assert(node.nkeys() == 1 && index == 0)
			new.setHeader(BNODE_NODE, 0)			
		} else {
			nodeReplaceKidN(tree, new, node, index, updated)
		}
	}

	return new
}

func nodeReplace2Kid(new BNode, node BNode, index uint16, ptr uint64, key []byte) {
	new.setHeader(node.btype(), node.nkeys() - 1)
	nodeAppendRange(new, node, 0, 0, index)
	nodeAppendKV(new, index, ptr, key, nil)
	nodeAppendRange(new, node, index, index + 2, node.nkeys() - index - 2)
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys() + right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func shouldMerge(tree *BTree, node BNode, index uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE / 4 {
		return 0, BNode{}
	}

	if index > 0 {
		sibling := tree.get(node.getPtr(index - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged < BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if index < node.nkeys() - 1 {
		sibling := tree.get(node.getPtr(index + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged < BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	last := old.nkeys() - 1
	lastOffset := old.getOffset(last)
	for i := last; i >= 0; i-- {
		
		offset := HEADER + (old.nkeys() - i) * 6 + (lastOffset - old.getOffset(i))
		if offset > BTREE_PAGE_SIZE {
			break;
		}
		
		last = i;
	}

	right.setHeader(old.btype(), old.nkeys() - last)
	nodeAppendRange(right, old, 0, last, old.nkeys() - last)

	left.setHeader(old.btype(), last)
	nodeAppendRange(left, old, 0, 0, last)
}

func nodeSplit3(node BNode) (uint16, [3]BNode) {
	if node.nbytes() <= BTREE_PAGE_SIZE {		
		node.data = node.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{ node }
	}

	left := BNode{make([]byte, 2 * BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}	
	nodeSplit2(left, right, node)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{left, middle, right}
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, index uint16, kids ...BNode) {
	n := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys() + n - 1)
	nodeAppendRange(new, old, 0, 0, index)
	for i, kid := range kids {
		nodeAppendKV(new, index + uint16(i), tree.new(kid), old.getKey(0), nil)
	}

	nodeAppendRange(new, old, index + n, index + 1, old.nkeys() - (index + 1))
}