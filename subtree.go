package mmdbwriter

import (
	"fmt"
	"hash/maphash"
	"io"
)

// This file adapts bottom-up hash-consing from Filliâtre and Conchon,
// "Type-Safe Modular Hash-Consing" (2006):
// https://usr.lmf.cnrs.fr/~jcf/publis/hash-consing2.pdf
// References to the paper below refer to this work.

// A key is the ordered pair of child payloads and their packed record types.
// Node payloads are canonical IDs, data payloads are interned value references,
// and aliases retain their raw targets so those targets need not be visited first.
type subtreeKey [3]uint32

type subtreeSlot struct {
	// indexPlusOne is zero for an empty slot. The root's arena index is zero.
	indexPlusOne uint32
	// hash is the cached hash (the paper's hkey), not its unique identity tag. Matches
	// still require exact key comparison. Canonical IDs live in subtreeTable.ids.
	hash uint32
}

// subtreeTable retains only representative indexes and hash tags. Exact keys
// are reconstructed from the resident tree on a hash match. It owns no nodes
// or values and lives only for the canonicalization pass.
// Reconstructing keys keeps each slot at eight bytes.
// Unlike the paper's long-lived weak table, this table cannot keep otherwise
// dead nodes alive: the tree already owns every node visited during the pass.
type subtreeTable struct {
	tree      *Tree
	ids       []uint32
	slots     []subtreeSlot
	seed      maphash.Seed
	used      int
	distinct  uint32
	protected nodeIndex
}

// subtreeIPv4Root finds the reader's IPv4 entry point even when aliasing is
// disabled and the entry record is not a FixedNode. Sharing this node with an
// unrelated subtree would cause alias-skipping iterators to omit that subtree.
// Call this after path expansion.
func subtreeIPv4Root(tree *Tree) nodeIndex {
	if tree.ipVersion != 6 {
		return noNodeIndex
	}
	index := tree.root
	for range 96 {
		r := tree.nodeAt(index).children[0]
		if r.recordType != recordTypeNode && r.recordType != recordTypeFixedNode {
			return noNodeIndex
		}
		index = r.nodeIndex
	}
	return index
}

// key reconstructs a visited representative's key. visit builds the same key
// while recursing so newly visited nodes do not need a second scan.
func (s *subtreeTable) key(index nodeIndex) subtreeKey {
	n := s.tree.nodeAt(index)
	var key subtreeKey
	for i := range 2 {
		r := &n.children[i]
		key[2] |= uint32(r.recordType) << (8 * i)
		switch r.recordType {
		case recordTypeData:
			key[i] = uint32(r.value)
		case recordTypeNode, recordTypeFixedNode:
			key[i] = s.ids[r.nodeIndex]
		case recordTypeAlias:
			key[i] = uint32(r.nodeIndex)
		case recordTypeEmpty, recordTypeReserved:
		default:
			panicUnexpectedSubtreeRecord(index, r.recordType)
		}
	}
	return key
}

// Keep formatting and its arguments out of the successful traversal path.
func panicUnexpectedSubtreeRecord(index nodeIndex, kind recordType) {
	panic(fmt.Sprintf(
		"mmdbwriter: unexpected record type %d at node %d during subtree canonicalization",
		kind,
		index,
	))
}

func (s *subtreeTable) hash(key subtreeKey) uint32 {
	// Seed randomization changes probe placement, not first-encounter IDs or
	// output order. Equality is always checked, including on hash collisions.
	//nolint:gosec // Truncating a hash is intentional.
	return uint32(maphash.Comparable(s.seed, key))
}

func (s *subtreeTable) grow() {
	old := s.slots
	s.slots = make([]subtreeSlot, 2*len(old))
	//nolint:gosec // The mask addresses the power-of-two table's uint32 indexes.
	mask := uint32(len(s.slots) - 1)
	for _, entry := range old {
		if entry.indexPlusOne == 0 {
			continue
		}
		bucket := entry.hash & mask
		for s.slots[bucket].indexPlusOne != 0 {
			bucket = (bucket + 1) & mask
		}
		s.slots[bucket] = entry
	}
}

func (s *subtreeTable) intern(index nodeIndex, key subtreeKey, hash uint32) uint32 {
	//nolint:gosec // The mask addresses the power-of-two table's uint32 indexes.
	mask := uint32(len(s.slots) - 1)
	bucket := hash & mask
	for entry := s.slots[bucket]; entry.indexPlusOne != 0; entry = s.slots[bucket] {
		if entry.hash == hash && s.key(nodeIndex(entry.indexPlusOne-1)) == key {
			id := s.ids[entry.indexPlusOne-1]
			// Prefer the latest matching node: its records and child IDs are
			// more likely to remain in cache than the first occurrence.
			s.slots[bucket].indexPlusOne = uint32(index) + 1
			return id
		}
		bucket = (bucket + 1) & mask
	}
	// Grow only on a miss, so repeated subtrees don't cause needless growth.
	if s.used >= len(s.slots)*3/4 {
		s.grow()
		return s.intern(index, key, hash)
	}
	s.slots[bucket] = subtreeSlot{indexPlusOne: uint32(index) + 1, hash: hash}
	s.used++
	s.distinct++
	return s.distinct
}

// visit propagates proof of uniqueness from data records with a sole owner.
// Container and caller-cache references can only prevent this shortcut from
// firing. Children are handled explicitly to avoid loop and key-packing overhead
// on every node, including those for which the shortcut does not apply.
func (s *subtreeTable) visit(index nodeIndex) (uint32, bool) {
	n := s.tree.nodeAt(index)
	var key subtreeKey
	unique := false
	key[2] = uint32(n.children[0].recordType) | uint32(n.children[1].recordType)<<8

	left := &n.children[0]
	switch left.recordType {
	case recordTypeData:
		key[0] = uint32(left.value)
		unique = s.tree.valueStore.nodes[left.value].refCount == 1
	case recordTypeNode, recordTypeFixedNode:
		key[0], unique = s.visit(left.nodeIndex)
	case recordTypeAlias:
		key[0] = uint32(left.nodeIndex)
	case recordTypeEmpty, recordTypeReserved:
	default:
		panicUnexpectedSubtreeRecord(index, left.recordType)
	}

	right := &n.children[1]
	switch right.recordType {
	case recordTypeData:
		key[1] = uint32(right.value)
		unique = unique || s.tree.valueStore.nodes[right.value].refCount == 1
	case recordTypeNode, recordTypeFixedNode:
		var rightUnique bool
		key[1], rightUnique = s.visit(right.nodeIndex)
		unique = unique || rightUnique
	case recordTypeAlias:
		key[1] = uint32(right.nodeIndex)
	case recordTypeEmpty, recordTypeReserved:
	default:
		panicUnexpectedSubtreeRecord(index, right.recordType)
	}
	var id uint32
	if unique || index == s.protected {
		// Unique subtrees cannot match another owning subtree. The IPv4 entry
		// must also remain distinct, but protection alone is not a uniqueness
		// proof. Descendants have already been canonicalized in either case.
		s.distinct++
		id = s.distinct
	} else {
		id = s.intern(index, key, s.hash(key))
	}
	s.ids[index] = id
	return id, unique
}

// canonicalizeSubtrees uses bottom-up hash-consing: children receive canonical
// IDs before their parent is interned by its ordered child records. Sharing is
// represented in the numbering side table. The tree's records remain unchanged.
//
// The paper hash-conses immutable values in smart constructors. Deferring this
// work until writing avoids insertion-time hashing and shared mutable nodes,
// which would need copy-on-write or equivalent mutation and lifetime tracking.
// Canonical IDs serve as the paper's unique tags only within this pass. Final
// numbering replaces them, and insertion invalidates the cached numbering.
//
// See Sections 1-2 and 4 of the paper linked at the top of this file.
func (t *Tree) canonicalizeSubtrees() int {
	table := subtreeTable{
		tree:      t,
		ids:       t.nodeNumbers,
		slots:     make([]subtreeSlot, 256),
		seed:      maphash.MakeSeed(),
		protected: subtreeIPv4Root(t),
	}
	table.visit(t.root)
	return int(table.distinct)
}

func (t *Tree) finalizeSubtrees() {
	// Use an arena-sized side table for canonical IDs first, then
	// replace them with final numbers. Before renumbering, zero marks
	// unreachable/retired slots. Afterward, zero is also the root's number.
	t.nodeNumbers = make([]uint32, t.nodeCountAllocated)
	// The interning table is unreachable before the numbering array is made.
	distinct := t.canonicalizeSubtrees()
	numbers := make([]uint32, distinct)
	t.nodeCount = int(t.numberSubtree(t.root, numbers, 0))
	for i, id := range t.nodeNumbers {
		if id != 0 {
			t.nodeNumbers[i] = numbers[id-1] - 1
		}
	}
}

// MMDB nodes implicitly consume the next address bit. A parent with two equal
// internal children must remain: the paper's binary decision diagram (BDD)
// reduction (Section 3.3) would skip that bit and change the lookup result.
func (t *Tree) numberSubtree(index nodeIndex, numbers []uint32, next uint32) uint32 {
	id := t.nodeNumbers[index]
	if numbers[id-1] != 0 {
		return next
	}
	// Store number+1 because node zero is valid. Allocation bounds the number
	// of live nodes below the uint32 sentinel, leaving room for this encoding.
	next++
	numbers[id-1] = next
	n := t.nodeAt(index)
	for i := range 2 {
		r := &n.children[i]
		if r.recordType == recordTypeNode || r.recordType == recordTypeFixedNode {
			next = t.numberSubtree(r.nodeIndex, numbers, next)
		}
	}
	return next
}

func (t *Tree) writeSubtree(
	w io.Writer,
	index nodeIndex,
	dw *dataWriter,
	buf []byte,
	next *uint32,
) (int64, error) {
	// Each search node consumes one address bit. The stack holds the next node
	// plus at most one pending right sibling per ancestor. IPv6 search nodes
	// occupy depths 0 through 127, so at most 128 entries are needed.
	var pending [128]nodeIndex
	pending[0] = index
	depth := 1
	var numBytes int64
	for depth > 0 {
		depth--
		index = pending[depth]
		number := t.nodeNumbers[index]
		if number < *next {
			continue
		}
		if number != *next {
			return numBytes, fmt.Errorf("node %d numbered %d but %d expected", index, number, *next)
		}
		n := t.nodeAt(index)
		if err := t.copyNode(buf, n, dw); err != nil {
			return numBytes, err
		}
		nb, err := w.Write(buf)
		numBytes += int64(nb)
		if err != nil {
			return numBytes, fmt.Errorf("writing node: %w", err)
		}
		*next++
		right := &n.children[1]
		if right.recordType == recordTypeNode || right.recordType == recordTypeFixedNode {
			pending[depth] = right.nodeIndex
			depth++
		}
		left := &n.children[0]
		if left.recordType == recordTypeNode || left.recordType == recordTypeFixedNode {
			pending[depth] = left.nodeIndex
			depth++
		}
	}
	return numBytes, nil
}
