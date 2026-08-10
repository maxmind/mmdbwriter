package mmdbwriter

import (
	"fmt"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type recordType byte

const (
	recordTypeEmpty recordType = iota
	recordTypeData
	recordTypeNode
	recordTypeAlias
	recordTypeFixedNode
	recordTypeReserved
	recordTypePath
)

type record struct {
	value valueRef
	// nodeIndex indexes Tree node blocks for node-like records and Tree.paths
	// for compressed-path records.
	nodeIndex nodeIndex

	recordType recordType
}

// each node contains two records.
type node struct {
	children [2]record
}

type compressedPath struct {
	ip       [16]byte
	record   record
	endDepth int
}

type nodeIndex uint32

const (
	rootNodeIndex nodeIndex = 0
	noNodeIndex             = ^nodeIndex(0)
	nodeBlockSize           = 1024
)

// insertRecord carries the state for one insert call. Every field except ip,
// prefixLen, and the memo fields is fixed for the life of the value;
// insertPrepared re-targets ip and prefixLen for each subnet of a range, and
// resolve updates the memo as it goes.
//
// The memo is only used for pure inserters. It is keyed on the existing value
// alone, which is sound only because inserter and value are fixed: reusing an
// insertRecord across inserts with a different value would return results
// computed from the previous one.
//
// The memo owns one store reference per key and per non-nil result. Every
// caller must run releaseResolved once insertion finishes. It releases the
// memo references and the reference interned for the inserted value itself.
// memoFirst and memoResult hold the single entry until a second distinct key
// promotes both into memo. From that point they are stale, so readers must
// check memo first.
type insertRecord struct {
	inserter func(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error)

	store        *valueStore
	tree         *Tree
	insertedNode nodeIndex

	ip        [16]byte
	prefixLen int

	recordType recordType
	value      valueRef
	// valueView is the value handed to the inserter as its new-value argument:
	// the caller's value as passed for the insert entry points, or a
	// store-materialized view when the insert began from an interned
	// reference, as when loading.
	valueView mmdbtype.DataType
	// callerValue is the caller's object for a direct insert. Its identity is
	// registered only after the insert succeeds, so a failed insert cannot
	// serve stale data if the caller mutates and retries the object.
	callerValue  mmdbtype.DataType
	inserterPure bool
	memo         map[valueRef]valueRef
	memoFirst    valueRef
	memoResult   valueRef
	memoSet      bool
}

// resolveValue returns the reference for a record and whether the caller owns
// it. With no inserter the reference interned when the insert began is reused
// directly; the store canonicalizes by content, so an equal existing value is
// the same reference and replaceDataRecord makes the assignment a no-op.
func (iRec *insertRecord) resolveValue(existing valueRef) (valueRef, bool, error) {
	if iRec.inserter != nil {
		return iRec.resolve(existing)
	}
	return iRec.value, false, nil
}

// resolve returns a reference and whether the caller owns it. A pure
// inserter's memo owns one reference to each non-nil result until insertion
// finishes. An ordinary Func is not memoized, so a newly interned reference is
// transferred directly to the target record.
func (iRec *insertRecord) resolve(existing valueRef) (valueRef, bool, error) {
	if iRec.inserterPure {
		if iRec.memo != nil {
			if value, ok := iRec.memo[existing]; ok {
				return value, false, nil
			}
		} else if iRec.memoSet && iRec.memoFirst == existing {
			return iRec.memoResult, false, nil
		}
	}
	result, err := iRec.inserter(iRec.store.materialize(existing), iRec.valueView)
	if err != nil {
		return nilValueRef, false, err
	}
	if result == nil {
		if iRec.inserterPure {
			iRec.rememberResolved(existing, nilValueRef)
		}
		return nilValueRef, false, nil
	}

	// A result equal to the existing value interns to the existing reference,
	// as intern canonicalizes by content, and the assignment in
	// replaceDataRecord then becomes a no-op. Interning is wire-exact, so
	// values that differ only in a float sign bit or NaN payload get a new
	// reference and replace the old one.
	value, err := iRec.store.intern(result)
	if err != nil {
		return nilValueRef, false, err
	}
	if iRec.inserterPure {
		iRec.rememberResolved(existing, value)
		return value, false, nil
	}
	return value, true, nil
}

func (iRec *insertRecord) rememberResolved(existing, result valueRef) {
	// The memo owns a reference to each key. Without it, a released key ref
	// could be recycled for a new value and produce a false memo hit.
	iRec.store.retain(existing)
	// Keep the common one-result case allocation-free. A second distinct
	// existing value promotes the first entry into the map.
	if !iRec.memoSet {
		iRec.memoFirst = existing
		iRec.memoResult = result
		iRec.memoSet = true
		return
	}
	if iRec.memo == nil {
		iRec.memo = map[valueRef]valueRef{
			iRec.memoFirst: iRec.memoResult,
		}
	}
	iRec.memo[existing] = result
}

// releaseResolved releases every reference the insertRecord owns: the value
// interned when the insert began, the memoized inserter results, and the memo
// keys.
func (iRec *insertRecord) releaseResolved() {
	iRec.store.release(iRec.value)
	if iRec.memo != nil {
		for key, value := range iRec.memo {
			iRec.store.release(key)
			iRec.store.release(value)
		}
		return
	}
	if iRec.memoSet {
		iRec.store.release(iRec.memoFirst)
		iRec.store.release(iRec.memoResult)
	}
}

func (iRec *insertRecord) replaceDataRecord(
	r *record,
	value valueRef,
	owned bool,
) {
	oldValue := r.value
	r.nodeIndex = iRec.insertedNode
	if value == nilValueRef {
		r.recordType = recordTypeEmpty
		r.value = nilValueRef
		iRec.store.release(oldValue)
		return
	}

	r.recordType = recordTypeData
	if oldValue != value {
		if !owned {
			iRec.store.retain(value)
		}
		r.value = value
		iRec.store.release(oldValue)
		return
	}
	if owned {
		iRec.store.release(value)
	}
}

func newNodeIndex(index int) nodeIndex {
	if index < 0 {
		panic("node index is negative")
	}
	if uint64(index) >= uint64(noNodeIndex) {
		panic("node index exceeds usable range")
	}
	return nodeIndex(index)
}

func (t *Tree) newNode(children [2]record) nodeIndex {
	index := newNodeIndex(t.nodeCountAllocated)
	if t.nodeCountAllocated == len(t.nodeBlocks)*nodeBlockSize {
		t.nodeBlocks = append(t.nodeBlocks, make([]node, nodeBlockSize))
	}
	// Node blocks are never reallocated, which keeps node pointers stable while
	// insertion allocates more nodes. Dead nodes are not reclaimed.
	t.nodeCountAllocated++
	*t.nodeAt(index) = node{children: children}
	return index
}

func (t *Tree) nodeAt(index nodeIndex) *node {
	return &t.nodeBlocks[int(index)/nodeBlockSize][int(index)%nodeBlockSize]
}

// newPath stores a compressed path for a sparse insertion. This avoids
// allocating one node per remaining bit until a later insert reaches the path
// or finalize expands it. Path entries are not reclaimed after materialization.
func (t *Tree) newPath(ip [16]byte, endDepth int, record record) nodeIndex {
	index := newNodeIndex(len(t.paths))
	t.paths = append(t.paths, compressedPath{
		ip:       ip,
		endDepth: endDepth,
		record:   record,
	})
	return index
}

// materializePath expands a compressed path into ordinary nodes starting at
// startDepth. The caller replaces the path record with the returned record.
func (t *Tree) materializePath(startDepth int, path compressedPath) record {
	child := path.record
	for depth := path.endDepth - 1; depth >= startDepth; depth-- {
		var children [2]record
		children[bitAt(path.ip, depth)] = child
		child = record{
			nodeIndex:  t.newNode(children),
			recordType: recordTypeNode,
		}
	}
	return child
}

func (iRec *insertRecord) insertNode(index nodeIndex, currentDepth int) error {
	newDepth := currentDepth + 1
	node := iRec.tree.nodeAt(index)
	// Check if we are inside the network already
	if newDepth > iRec.prefixLen {
		// Data already exists for the network so insert into all the children.
		// Identical child records are merged as recursion unwinds.
		err := iRec.insertRecord(&node.children[0], newDepth)
		if err != nil {
			return err
		}
		return iRec.insertRecord(&node.children[1], newDepth)
	}

	// We haven't reached the network yet.
	pos := bitAt(iRec.ip, currentDepth)
	return iRec.insertRecord(&node.children[pos], newDepth)
}

func (iRec *insertRecord) insertRecord(
	r *record,
	newDepth int,
) error {
	switch r.recordType {
	case recordTypeNode:
		err := iRec.insertNode(r.nodeIndex, newDepth)
		if err != nil {
			return err
		}
		return iRec.maybeMergeChildren(r)
	case recordTypeFixedNode:
		return iRec.insertNode(r.nodeIndex, newDepth)
	case recordTypePath:
		path := iRec.tree.paths[r.nodeIndex]
		*r = iRec.tree.materializePath(newDepth, path)
		return iRec.insertRecord(r, newDepth)
	case recordTypeEmpty, recordTypeData:
		if newDepth >= iRec.prefixLen {
			if iRec.recordType == recordTypeData {
				value, owned, err := iRec.resolveValue(r.value)
				if err != nil {
					return err
				}
				iRec.replaceDataRecord(r, value, owned)
			} else {
				oldValue := r.value
				r.nodeIndex = iRec.insertedNode
				r.recordType = iRec.recordType
				r.value = nilValueRef
				iRec.store.release(oldValue)
			}
			return nil
		}

		if r.recordType == recordTypeEmpty && iRec.recordType == recordTypeData {
			value, owned, err := iRec.resolveValue(nilValueRef)
			if err != nil {
				return err
			}
			if value == nilValueRef {
				return nil
			}
			if !owned {
				iRec.store.retain(value)
			}
			r.nodeIndex = iRec.tree.newPath(iRec.ip, iRec.prefixLen, record{
				value:      value,
				recordType: recordTypeData,
			})
			r.recordType = recordTypePath
			return nil
		}

		// We are splitting this record so we create two duplicate child
		// records.
		if r.recordType == recordTypeData {
			iRec.store.retain(r.value)
		}
		r.nodeIndex = iRec.tree.newNode([2]record{*r, *r})
		r.value = nilValueRef
		r.recordType = recordTypeNode
		err := iRec.insertNode(r.nodeIndex, newDepth)
		if err != nil {
			return err
		}
		return iRec.maybeMergeChildren(r)
	case recordTypeReserved:
		if iRec.prefixLen >= newDepth {
			return newReservedNetworkError(iRec.ip, newDepth, iRec.prefixLen, iRec.tree.treeDepth)
		}
		// If we are inserting a network that contains a reserved network,
		// we silently remove the reserved network.
		return nil
	case recordTypeAlias:
		if iRec.prefixLen < newDepth {
			// Do nothing. We are inserting a network that contains an aliased
			// network. We silently ignore.
			return nil
		}
		// attempting to insert _into_ an aliased network
		return newAliasedNetworkError(iRec.ip, newDepth, iRec.prefixLen, iRec.tree.treeDepth)
	default:
		return fmt.Errorf("inserting into record type %d is not implemented", r.recordType)
	}
}

func (iRec *insertRecord) maybeMergeChildren(r *record) error {
	// Check to see if the children are the same and can be merged.
	// Use pointer access to avoid copying the record struct; this is
	// called from every node-level insert, so the copies add up across
	// millions of inserts.
	node := iRec.tree.nodeAt(r.nodeIndex)
	child0 := &node.children[0]
	child1 := &node.children[1]
	if child0.recordType != child1.recordType {
		return nil
	}
	switch child0.recordType {
	// Node-like and compressed-path records can't be merged by record equality.
	case recordTypeFixedNode, recordTypeNode, recordTypePath:
		return nil
	case recordTypeEmpty, recordTypeReserved:
		r.recordType = child0.recordType
		r.nodeIndex = noNodeIndex
		return nil
	case recordTypeData:
		// The store keeps exactly one live node per wire-equal value, so
		// reference equality here is value equality.
		if child0.value != child1.value {
			return nil
		}
		// Children have same data and can be merged
		r.recordType = recordTypeData
		r.value = child0.value
		iRec.store.release(child1.value)
		r.nodeIndex = noNodeIndex
		return nil
	default:
		return fmt.Errorf("merging record type %d is not implemented", child0.recordType)
	}
}

func (t *Tree) getNode(
	index nodeIndex,
	ip [16]byte,
	depth int,
) (int, record) {
	n := t.nodeAt(index)
	r := n.children[bitAt(ip, depth)]

	depth++

	return t.getRecord(r, ip, depth)
}

func (t *Tree) getRecord(
	r record,
	ip [16]byte,
	depth int,
) (int, record) {
	if r.recordType == recordTypePath {
		path := t.paths[r.nodeIndex]
		for pathDepth := depth; pathDepth < path.endDepth; pathDepth++ {
			if bitAt(ip, pathDepth) != bitAt(path.ip, pathDepth) {
				return pathDepth + 1, record{}
			}
		}
		return t.getRecord(path.record, ip, path.endDepth)
	}

	switch r.recordType {
	case recordTypeNode, recordTypeAlias, recordTypeFixedNode:
		return t.getNode(r.nodeIndex, ip, depth)
	default:
		return depth, r
	}
}

func (t *Tree) expandPaths(index nodeIndex, currentDepth int) {
	n := t.nodeAt(index)
	for i := range 2 {
		child := &n.children[i]
		recordDepth := currentDepth + 1
		switch child.recordType {
		case recordTypePath:
			path := t.paths[child.nodeIndex]
			*child = t.materializePath(recordDepth, path)
			if child.recordType == recordTypeNode {
				t.expandPaths(child.nodeIndex, recordDepth)
			}
		case recordTypeNode, recordTypeFixedNode:
			t.expandPaths(child.nodeIndex, recordDepth)
		case recordTypeEmpty, recordTypeData, recordTypeAlias, recordTypeReserved:
		}
	}
}

// finalizeNode assigns node numbers depth-first. expandPaths must run before
// this so compressed paths cannot be confused with node indexes.
func (t *Tree) finalizeNode(index nodeIndex, currentNum int) int {
	n := t.nodeAt(index)
	t.nodeNumbers[index] = currentNum
	currentNum++

	for i := range 2 {
		switch n.children[i].recordType {
		case recordTypeFixedNode,
			recordTypeNode:
			currentNum = t.finalizeNode(n.children[i].nodeIndex, currentNum)
		case recordTypePath:
			panic("compressed path found after expandPaths")
		default:
		}
	}

	return currentNum
}

func bitAt(ip [16]byte, depth int) byte {
	return (ip[depth/8] >> (7 - (depth % 8))) & 1
}
