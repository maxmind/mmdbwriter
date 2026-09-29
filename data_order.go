package mmdbwriter

import (
	"cmp"
	"fmt"
	"slices"
)

// assumedPointerSize is the pointer size that the score assumes. Pointers to
// the first 526,336 bytes, where the order matters most, take at most 3.
const assumedPointerSize = 3

// orderRecordValues sorts records in place into the order that WriteTo writes
// them, and takes ownership of the slice. treeRefs counts the data records that
// hold each value.
//
// A value goes into the data section once for each container slot that holds
// it: its refCount minus its treeRefs. Each value is credited to its first
// holder in first-seen order. A record's score is the pointer uses of its
// credited values per byte that it adds, so records with the most reused values
// come first and get short pointers. Other records and ties keep first-seen
// order.
func orderRecordValues(store *valueStore, records []valueRef, treeRefs []uint32) {
	o := recordOrderer{
		store:    store,
		treeRefs: treeRefs,
		cost:     make([]uint8, len(store.nodes)),
	}
	type scored struct {
		ref   valueRef
		index uint32
		score float64
	}
	// Only records with a gain are sorted. The loop packs the others, in order,
	// at the front of records, over entries that it has already read.
	var entries []scored
	noGain := 0
	for i, ref := range records {
		o.gain, o.size = 0, 0
		o.visit(ref, true)
		if o.gain == 0 {
			records[noGain] = ref
			noGain++
			continue
		}
		entries = append(entries, scored{
			ref:   ref,
			index: uint32(i),
			score: float64(o.gain) / float64(o.size),
		})
	}
	slices.SortFunc(entries, func(a, b scored) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		return cmp.Compare(a.index, b.index)
	})
	copy(records[len(entries):], records[:noGain])
	for i, entry := range entries {
		records[i] = entry.ref
	}
}

type recordOrderer struct {
	store    *valueStore
	treeRefs []uint32
	// cost is zero for a value that no earlier record holds. Otherwise it is
	// the number of bytes that a later reference to the value takes.
	cost []uint8
	gain uint64
	size uint64
}

// visit adds the record's gain and size to o.gain and o.size, and marks its
// new values as written. Like the writer, it writes a new value in full and
// refers to a written one.
func (o *recordOrderer) visit(ref valueRef, root bool) {
	if c := o.cost[ref]; c != 0 {
		// A tree record points to its value's first copy, so a written root
		// adds nothing.
		if !root {
			o.size += uint64(c)
		}
		return
	}
	node := &o.store.nodes[ref]
	start := o.size
	if isContainer(node) {
		o.size += containerHeaderSize(node.kind, containerEntries(node))
		for _, child := range o.store.childRefs(node) {
			o.visit(child, false)
		}
	} else {
		o.size += uint64(node.payloadLen)
	}
	size := o.size - start
	o.cost[ref] = referenceCost(size)
	o.gain += o.pointerUses(ref, node, size, root)
}

// pointerUses returns the pointer uses that writing the value in full makes
// possible. A value no larger than a pointer never gets one. A record's root
// is written at the top level, so every container slot that holds it can
// point to it. A nested value fills one slot itself.
func (o *recordOrderer) pointerUses(ref valueRef, node *valueNode, size uint64, root bool) uint64 {
	if size <= assumedPointerSize {
		return 0
	}
	treeRefs := o.treeRefs[ref]
	if treeRefs > node.refCount {
		panicTreeRefs(ref, treeRefs, node.refCount)
	}
	slots := uint64(node.refCount - treeRefs)
	if root || slots == 0 {
		return slots
	}
	return slots - 1
}

// Keep formatting out of the scoring path.
func panicTreeRefs(ref valueRef, treeRefs, refCount uint32) {
	panic(fmt.Sprintf(
		"mmdbwriter: value %d is held by %d data records but has a refcount of %d",
		ref,
		treeRefs,
		refCount,
	))
}

// referenceCost returns the size of a reference to a written value: a
// pointer, or the value itself if it is no larger.
func referenceCost(size uint64) uint8 {
	if size > assumedPointerSize {
		return assumedPointerSize
	}
	return uint8(size)
}
