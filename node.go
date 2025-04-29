package mmdbwriter

import (
	"fmt"
	"math"
	"net"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

type recordType byte

const (
	recordTypeUnused recordType = iota
	recordTypeEmpty
	recordTypeData
	recordTypeNode
	recordTypeAlias
	recordTypeFixedNode
	recordTypeReserved
)

const nonExistentNode = math.MaxUint32

type record struct {
	node       uint32
	recordType recordType
	value      *dataMapValue
}

// each node contains two records.
type node struct {
	children [2]record
	nodeNum  int
}

type insertRecord struct {
	inserter func(value mmdbtype.DataType) (mmdbtype.DataType, error)

	dataMap      *dataMap
	insertedNode uint32

	nodes *nodes

	ip        net.IP
	prefixLen int

	recordType recordType
}

func (n *node) insert(iRec insertRecord, currentDepth int) error {
	newDepth := currentDepth + 1
	// Check if we are inside the network already
	if newDepth > iRec.prefixLen {
		// Data already exists for the network so insert into all the children.
		// We will prune duplicate nodes when we finalize.
		err := n.children[0].insert(iRec, newDepth)
		if err != nil {
			return err
		}
		return n.children[1].insert(iRec, newDepth)
	}

	// We haven't reached the network yet.
	pos := bitAt(iRec.ip, currentDepth)
	r := &n.children[pos]
	return r.insert(iRec, newDepth)
}

func (r *record) insert(
	iRec insertRecord,
	newDepth int,
) error {
	switch r.recordType {
	case recordTypeNode, recordTypeFixedNode:
		recNode, err := iRec.nodes.get(r.node)
		if err != nil {
			return err
		}
		err = recNode.insert(iRec, newDepth)
		if err != nil {
			return err
		}
		return r.maybeMergeChildren(iRec)
	case recordTypeEmpty, recordTypeData:
		if newDepth >= iRec.prefixLen {
			r.node = iRec.insertedNode
			r.recordType = iRec.recordType
			switch iRec.recordType {
			case recordTypeData:
				var oldData mmdbtype.DataType
				if r.value != nil {
					oldData = r.value.data
				}
				newData, err := iRec.inserter(oldData)
				if err != nil {
					return err
				}
				if newData == nil {
					iRec.dataMap.remove(r.value)
					r.recordType = recordTypeEmpty
					r.value = nil
				} else if oldData == nil || !oldData.Equal(newData) {
					iRec.dataMap.remove(r.value)
					value, err := iRec.dataMap.store(newData)
					//nolint:revive //preexisting
					if err != nil {
						return err
					}
					r.value = value
				}
			case recordTypeFixedNode:
				var newNode *node
				newNode, r.node = iRec.nodes.acquireNode()
				newNode.children[0].recordType = recordTypeEmpty
				newNode.children[1].recordType = recordTypeEmpty
				r.value = nil
			default:
				r.value = nil
			}
			return nil
		}

		// We are splitting this record so we create two duplicate child
		// records.
		var newNode *node
		newNode, r.node = iRec.nodes.acquireNode()
		for i := range 2 {
			newNode.children[i].value = r.value
			newNode.children[i].recordType = r.recordType
		}
		r.value = nil
		r.recordType = recordTypeNode
		err := newNode.insert(iRec, newDepth)
		if err != nil {
			return err
		}
		return r.maybeMergeChildren(iRec)
	case recordTypeReserved:
		if iRec.prefixLen >= newDepth {
			return newReservedNetworkError(iRec.ip, newDepth, iRec.prefixLen)
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
		return newAliasedNetworkError(iRec.ip, newDepth, iRec.prefixLen)
	default:
		return fmt.Errorf("inserting into record type %d for node %d is not implemented", r.recordType, r.node)
	}
}

func (r *record) maybeMergeChildren(iRec insertRecord) error {
	if r.recordType == recordTypeFixedNode {
		return nil
	}
	recNode, err := iRec.nodes.get(r.node)
	if err != nil {
		return err
	}

	// Check to see if the children are the same and can be merged.
	child0 := recNode.children[0]
	child1 := recNode.children[1]
	if child0.recordType != child1.recordType {
		return nil
	}
	switch child0.recordType {
	// Nodes can't be merged
	case recordTypeFixedNode, recordTypeNode:
		return nil
	case recordTypeEmpty, recordTypeReserved:
		r.recordType = child0.recordType
		err := iRec.nodes.returnNode(r.node)
		if err != nil {
			return err
		}
		r.node = nonExistentNode
		return nil
	case recordTypeData:
		if child0.value.key != child1.value.key {
			return nil
		}
		// Children have same data and can be merged
		r.recordType = recordTypeData
		r.value = child0.value
		iRec.dataMap.remove(child1.value)
		err := iRec.nodes.returnNode(r.node)
		if err != nil {
			return err
		}
		r.node = nonExistentNode
		return nil
	default:
		return fmt.Errorf("merging record type %d is not implemented", child0.recordType)
	}
}

func (n *node) get(
	ip net.IP,
	curDepth,
	maxDepth int,
	nodes *nodes,
) (int, record, error) {
	r := n.children[bitAt(ip, curDepth)]

	curDepth++

	if curDepth == maxDepth {
		return curDepth, r, nil
	}

	switch r.recordType {
	case recordTypeNode, recordTypeAlias, recordTypeFixedNode:
		recNode, err := nodes.get(r.node)
		if err != nil {
			return 0, record{}, err
		}

		return recNode.get(ip, curDepth, maxDepth, nodes)
	default:
		return curDepth, r, nil
	}
}

// finalize sets the node number for the node. It returns the current node
// count, including the subtree.
func (n *node) finalize(currentNum int, nodes *nodes) (int, error) {
	n.nodeNum = currentNum
	currentNum++

	for i := range 2 {
		switch n.children[i].recordType {
		case recordTypeFixedNode,
			recordTypeNode:
			recNode, err := nodes.get(n.children[i].node)
			if err != nil {
				return 0, err
			}

			currentNum, err = recNode.finalize(currentNum, nodes)
			if err != nil {
				return 0, err
			}
		default:
		}
	}

	return currentNum, nil
}

func (n *node) reset() {
	// This is done to make bugs easier to track down. If we leave it at zero, it
	// tends to get confused with the root node
	n.children[0].node = nonExistentNode
	n.children[0].recordType = recordTypeUnused
	n.children[1].node = nonExistentNode
	n.children[1].recordType = recordTypeUnused
}

func bitAt(ip net.IP, depth int) byte {
	return (ip[depth/8] >> (7 - (depth % 8))) & 1
}
