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
	value *dataMapValue
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

type insertRecord struct {
	inserter func(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error)

	dataMap      *dataMap
	tree         *Tree
	insertedNode nodeIndex

	ip        [16]byte
	prefixLen int

	recordType recordType
	value      mmdbtype.DataType
}

func (iRec insertRecord) storeData(v mmdbtype.DataType) (*dataMapValue, error) {
	if iRec.inserter == nil {
		return iRec.dataMap.storeWithIdentity(v)
	}
	return iRec.dataMap.store(v)
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

func (iRec insertRecord) insertNode(index nodeIndex, currentDepth int) error {
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

func (iRec insertRecord) insertRecord(
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
				var oldData mmdbtype.DataType
				if r.value != nil {
					oldData = r.value.data
				}
				newData := iRec.value
				if iRec.inserter != nil {
					var err error
					newData, err = iRec.inserter(oldData, iRec.value)
					if err != nil {
						return err
					}
				}
				switch {
				case newData == nil:
					iRec.dataMap.remove(r.value)
					r.nodeIndex = iRec.insertedNode
					r.recordType = recordTypeEmpty
					r.value = nil
				case oldData == nil || !oldData.Equal(newData):
					value, err := iRec.storeData(newData)
					if err != nil {
						return err
					}
					iRec.dataMap.remove(r.value)
					r.nodeIndex = iRec.insertedNode
					r.recordType = iRec.recordType
					r.value = value
				default:
					r.nodeIndex = iRec.insertedNode
					r.recordType = iRec.recordType
				}
			} else {
				oldValue := r.value
				r.nodeIndex = iRec.insertedNode
				r.recordType = iRec.recordType
				r.value = nil
				iRec.dataMap.remove(oldValue)
			}
			return nil
		}

		if r.recordType == recordTypeEmpty && iRec.recordType == recordTypeData {
			newData := iRec.value
			if iRec.inserter != nil {
				var err error
				newData, err = iRec.inserter(nil, iRec.value)
				if err != nil {
					return err
				}
			}
			if newData == nil {
				return nil
			}
			value, err := iRec.storeData(newData)
			if err != nil {
				return err
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
			iRec.dataMap.addRef(r.value)
		}
		r.nodeIndex = iRec.tree.newNode([2]record{*r, *r})
		r.value = nil
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

func (iRec insertRecord) maybeMergeChildren(r *record) error {
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
		if child0.value.key != child1.value.key {
			return nil
		}
		// Children have same data and can be merged
		r.recordType = recordTypeData
		r.value = child0.value
		iRec.dataMap.remove(child1.value)
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
