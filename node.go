package mmdbwriter

import (
	"fmt"
	"net"
)

type recordType byte

const (
	recordTypeEmpty recordType = iota
	recordTypeData
	recordTypeNode
	recordTypeAlias     // nolint: deadcode, varcheck
	recordTypeFixed     // nolint: deadcode, varcheck
	recordTypeImmutable // nolint: deadcode, varcheck
)

type record struct {
	node       *node
	value      DataType
	recordType recordType
}

// each node contains two records.
type node struct {
	children [2]record
	nodeNum  int
}

func (n *node) insert(
	ip net.IP,
	prefixLen int,
	recordType recordType,
	value DataType,
	currentDepth int,
) {
	pos := bitAt(ip, currentDepth)
	r := &n.children[pos]

	currentDepth++
	if currentDepth == prefixLen {
		r.node = nil
		r.value = value
		r.recordType = recordType
		return
	}

	switch r.recordType {
	case recordTypeNode:
	case recordTypeEmpty, recordTypeData:
		// We are splitting this record so we create two duplicate child
		// records.
		r.node = &node{children: [2]record{*r, *r}}
		r.value = nil
		r.recordType = recordTypeNode
	default:
		panic(fmt.Sprintf("record type %d not implemented!", r.recordType))
	}

	r.node.insert(ip, prefixLen, recordType, value, currentDepth)
}

func (n *node) get(
	ip net.IP,
	depth int,
) (int, *DataType) {
	r := n.children[bitAt(ip, depth)]

	depth++

	if r.value != nil {
		return depth, &r.value
	}

	if r.node == nil {
		return depth, nil
	}

	return r.node.get(ip, depth)
}

// finalize current just returns the node count. However, it will prune the
// tree eventually.
func (n *node) finalize(currentNum int) int {
	n.nodeNum = currentNum
	currentNum++
	if n.children[0].recordType != recordTypeNode &&
		n.children[1].recordType != recordTypeNode {
		return currentNum
	}

	if n.children[0].recordType == recordTypeNode {
		currentNum = n.children[0].node.finalize(currentNum)
	}

	if n.children[1].recordType == recordTypeNode {
		currentNum = n.children[1].node.finalize(currentNum)
	}
	return currentNum
}

func bitAt(ip net.IP, depth int) byte {
	return (ip[depth/8] >> (7 - (depth % 8))) & 1
}
