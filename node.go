package mmdbwriter

import (
	"errors"
	"fmt"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/internal/treeaddr"
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

// insertRecord carries the state for one insert call. Most fields are fixed for
// the life of the value. These are not:
//
//   - ip, prefixLen, and insertedAs4, which insertPrepared re-targets for each
//     subnet of a range.
//   - splitDepth, which insertPrepared resets for each subnet and traversal
//     sets when it splits a record.
//   - the memo fields, which resolve updates as it goes.
//
// The memo is only used for pure inserters. It is keyed on the existing value
// alone, which is sound only because the resolver and value are fixed: reusing an
// insertRecord across inserts with a different value would return results
// computed from the previous one.
//
// The memo owns one store reference per key and per non-nil result. Every
// caller must run releaseResolved once insertion finishes. It releases the
// memo references and the reference interned for the inserted value itself.
// memoFirst and memoResult hold the single entry until a second distinct key
// promotes both into memo, which clears them.
//
// Fields are ordered widest first. The struct is one allocation per insert
// call, and this layout keeps it inside the 128-byte size class.
type insertRecord struct {
	resolver insertResolver

	store *valueStore
	tree  *Tree

	// valueView is the value handed to the inserter as its new-value argument:
	// the caller's value as passed for the insert entry points, or a
	// store-materialized view when the insert began from an interned
	// reference, as when loading.
	valueView mmdbtype.DataType
	// callerValue is the caller's object for a direct insert. Its identity is
	// registered only after the insert succeeds, so a failed insert cannot
	// serve stale data if the caller mutates and retries the object.
	callerValue mmdbtype.DataType
	memo        map[valueRef]valueRef

	prefixLen int
	ip        [16]byte

	insertedNode nodeIndex
	value        valueRef
	memoFirst    valueRef
	memoResult   valueRef

	recordType recordType
	// insertedAs4 records the address family that the tree-space ip cannot
	// encode. A netip.Prefix field would carry it directly, but at 32 bytes
	// it pushes the struct into the next size class, while a pointer to a
	// prepared Metadata escapes to the heap once per insertRange call. The
	// flag fits in padding, and rebuilding the prefix per record measures at
	// about 4ns.
	insertedAs4 bool
	// splitDepth is the first pre-insert record depth in a split chain. Tree
	// depths are at most 128, and zero is the sentinel for no active split.
	splitDepth uint8
	memoSet    bool
}

// resolveValue returns the reference for a record and whether the caller owns
// it. With no inserter the reference interned when the insert began is reused
// directly: the store canonicalizes by content, so an equal existing value is
// the same reference and replaceDataRecord makes the assignment a no-op.
func (iRec *insertRecord) resolveValue(
	existing valueRef,
	existingDepth int,
) (valueRef, bool, error) {
	if !iRec.resolver.hasFunc() {
		return iRec.value, false, nil
	}
	return iRec.resolve(existing, existingDepth)
}

// resolve returns a reference and whether the caller owns it. A pure
// inserter's memo owns one reference to each non-nil result until insertion
// finishes. A metadata-aware Func is not memoized, so a newly interned
// reference is transferred directly to the target record.
func (iRec *insertRecord) resolve(
	existing valueRef,
	existingDepth int,
) (valueRef, bool, error) {
	if iRec.resolver.pure != nil {
		if iRec.memo != nil {
			if value, ok := iRec.memo[existing]; ok {
				return value, false, nil
			}
		} else if iRec.memoSet && iRec.memoFirst == existing {
			return iRec.memoResult, false, nil
		}
	}
	var result mmdbtype.DataType
	var err error
	if iRec.resolver.pure != nil {
		result, err = iRec.resolver.pure(
			iRec.store.materialize(existing),
			iRec.valueView,
		)
	} else {
		insertedNetwork, metadataErr := treeaddr.PrefixFromInsertIP(
			iRec.ip,
			iRec.prefixLen,
			iRec.tree.treeDepth,
			iRec.insertedAs4,
		)
		if metadataErr != nil {
			return nilValueRef, false, fmt.Errorf(
				"creating inserted network metadata: %w",
				metadataErr,
			)
		}
		metadata := inserter.Metadata{
			InsertedNetwork: insertedNetwork,
			ExistingDepth:   existingDepth,
			TreeDepth:       iRec.tree.treeDepth,
		}
		switch {
		case existingDepth == iRec.prefixLen:
			// ip is already masked at prefixLen, so no masking is needed.
			metadata.ExistingAddr = iRec.ip
		case existingDepth < iRec.prefixLen:
			metadata.ExistingAddr = maskedTreeAddr(iRec.ip, existingDepth)
		}
		result, err = iRec.resolver.withMetadata(
			iRec.store.materialize(existing),
			iRec.valueView,
			metadata,
		)
	}
	if err != nil {
		return nilValueRef, false, err
	}
	if result == nil {
		if iRec.resolver.pure != nil {
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
	if iRec.resolver.pure != nil {
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
	if iRec.memo == nil && !iRec.memoSet {
		iRec.memoFirst = existing
		iRec.memoResult = result
		iRec.memoSet = true
		return
	}
	if iRec.memo == nil {
		iRec.memo = map[valueRef]valueRef{
			iRec.memoFirst: iRec.memoResult,
		}
		// Clear the single-entry fields at promotion, so a reader that
		// forgets to check the map first reads nil instead of a stale hit.
		iRec.memoFirst = nilValueRef
		iRec.memoResult = nilValueRef
		iRec.memoSet = false
	}
	iRec.memo[existing] = result
}

// releaseResolved releases every reference the insertRecord owns: the value
// interned when the insert began, the memoized inserter results, and the memo
// keys. It detaches every field before releasing anything, so a release panic
// cannot make the deferred second call retry the same reference and mask the
// original failure. A second call is otherwise a no-op. That lets callers both
// defer it for panic safety and call it explicitly before the audit runs.
func (iRec *insertRecord) releaseResolved() {
	value := iRec.value
	memo := iRec.memo
	memoFirst := iRec.memoFirst
	memoResult := iRec.memoResult
	memoSet := iRec.memoSet

	iRec.value = nilValueRef
	iRec.memo = nil
	iRec.memoFirst = nilValueRef
	iRec.memoResult = nilValueRef
	iRec.memoSet = false

	iRec.store.release(value)
	if memo != nil {
		for key, value := range memo {
			iRec.store.release(key)
			iRec.store.release(value)
		}
	} else if memoSet {
		iRec.store.release(memoFirst)
		iRec.store.release(memoResult)
	}
}

// replaceDataRecord is the guarded mutation path for a record's value: it
// releases the old value exactly once and stores the new one. The owned flag
// is resolve's ownership handoff. When the caller already owns the incoming
// reference, the record adopts it; otherwise the record retains its own. A
// value equal to the old one leaves the record untouched, releasing the
// incoming reference if it was owned.
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

func (iRec *insertRecord) insertNode(
	index nodeIndex,
	currentDepth int,
) error {
	// A split chain descends through one child from splitDepth to prefixLen:
	// splitting only happens above prefixLen, so the next insertNode takes the
	// single-child path. Below prefixLen, records resolve at their own depths
	// and are never split. The two depths are therefore never both live, and
	// iRec.splitDepth alone preserves the pre-insert record boundary without
	// adding a parameter to every recursive call.
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
			return iRec.mergeChildrenAfterError(r, err)
		}
		return iRec.maybeMergeChildren(r)
	case recordTypeFixedNode:
		return iRec.insertNode(r.nodeIndex, newDepth)
	case recordTypePath:
		path := iRec.tree.paths[r.nodeIndex]
		// materializePath moves the path record's value ownership into the
		// expanded nodes. Zero the dead slot, so an accidental later read
		// fails loudly instead of double-counting the moved reference.
		iRec.tree.paths[r.nodeIndex].record = record{}
		*r = iRec.tree.materializePath(newDepth, path)
		return iRec.insertRecord(r, newDepth)
	case recordTypeEmpty, recordTypeData:
		if newDepth >= iRec.prefixLen {
			if iRec.recordType == recordTypeData {
				existingDepth := newDepth
				if iRec.splitDepth != 0 {
					existingDepth = int(iRec.splitDepth)
				}
				value, owned, err := iRec.resolveValue(r.value, existingDepth)
				if err != nil {
					return err
				}
				iRec.replaceDataRecord(r, value, owned)
			} else {
				// This mirrors replaceDataRecord's release-then-overwrite for
				// a non-data target. It stays inline because the split case
				// below transfers the old reference instead of releasing it,
				// so a shared helper would cover only part of the pattern.
				oldValue := r.value
				r.nodeIndex = iRec.insertedNode
				r.recordType = iRec.recordType
				r.value = nilValueRef
				iRec.store.release(oldValue)
			}
			return nil
		}

		if r.recordType == recordTypeEmpty && iRec.recordType == recordTypeData {
			value, owned, err := iRec.resolveValue(nilValueRef, newDepth)
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
		if iRec.splitDepth == 0 {
			iRec.splitDepth = uint8(newDepth) //nolint:gosec // Tree depths cannot exceed 128.
		}
		if r.recordType == recordTypeData {
			iRec.store.retain(r.value)
		}
		r.nodeIndex = iRec.tree.newNode([2]record{*r, *r})
		r.value = nilValueRef
		r.recordType = recordTypeNode
		err := iRec.insertNode(r.nodeIndex, newDepth)
		if err != nil {
			return iRec.mergeChildrenAfterError(r, err)
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

func (iRec *insertRecord) mergeChildrenAfterError(r *record, insertErr error) error {
	mergeErr := iRec.maybeMergeChildren(r)
	if mergeErr == nil {
		return insertErr
	}
	return errors.Join(insertErr, mergeErr)
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
			// Zero the dead slot, as in insertRecord's path case.
			t.paths[child.nodeIndex].record = record{}
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

func maskedTreeAddr(ip [16]byte, depth int) [16]byte {
	byteIndex := depth / 8
	if remainingBits := depth % 8; remainingBits != 0 {
		ip[byteIndex] &= byte(0xff << (8 - remainingBits))
		byteIndex++
	}
	clear(ip[byteIndex:])
	return ip
}
