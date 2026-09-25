package mmdbwriter

import (
	"encoding/binary"
	"math/bits"
)

// insertCursor borrows an existing node path for reuse between inserts.
// nodes[depth] is the node consuming that address bit; only nodes[:length] are
// valid. It never holds records inside a compressed path or follows aliases.
// Its own merges truncate the path before a retired node index can be reused.
// Tree rejects reentrant mutation while this path is in use.
type insertCursor struct {
	nodes  [128]nodeIndex
	length int
	last   [16]byte
	valid  bool
}

func (t *Tree) insertWithCursor(iRec *insertRecord) error {
	c := &t.insertCursor
	if t.disableInsertCursor || iRec.recordType != recordTypeData || iRec.prefixLen == 0 {
		c.valid = false
		return iRec.insertNode(t.root, 0)
	}
	ip := iRec.ip
	depth := 0
	if c.valid {
		// Use normalized tree-space addresses, so IPv4, mapped IPv4, and
		// IPv6 share the same prefix comparison regardless of insertion order.
		// Stop above the target record even when its start is unchanged or
		// shares more address bits than its prefix length.
		depth = commonInsertPrefixBits(c.last, ip)
		depth = min(depth, c.length-1, iRec.prefixLen-1)
	}
	// Invalid until success, including when a caller's callback panics.
	c.valid = false
	c.nodes[0] = t.root
	n := t.nodeAt(c.nodes[depth])
	r := &n.children[bitAt(ip, depth)]
	depth++
	for depth < iRec.prefixLen && r.isOwningNode() {
		c.nodes[depth] = r.nodeIndex
		n = t.nodeAt(r.nodeIndex)
		r = &n.children[bitAt(ip, depth)]
		depth++
	}
	// No callback can observe the working path until descent is complete.
	c.length = depth
	// The existing recursive helpers handle splits, compressed paths,
	// callbacks, and traversal inside the target prefix. Without reentrant
	// mutations, that traversal cannot retire any of the cached ancestors.
	err := iRec.insertRecord(r, depth)
	// Merge before returning: Get and metadata callbacks observe record
	// boundaries between inserts. Once a child remains internal, no ancestor
	// can coalesce. Fixed nodes also stop merging to preserve alias targets.
	for depth > 1 && !r.isOwningNode() {
		depth--
		n = t.nodeAt(c.nodes[depth-1])
		r = &n.children[bitAt(ip, depth-1)]
		err = iRec.mergeChildrenAfterInsert(r, err)
		if !r.isOwningNode() {
			c.length = depth
		}
	}
	// insertNode may have changed iRec.ip while visiting covered records.
	// Cache the original normalized start, not the last visited address.
	c.last = ip
	c.valid = err == nil
	return err
}

func commonInsertPrefixBits(a, b [16]byte) int {
	high := binary.BigEndian.Uint64(a[:8]) ^ binary.BigEndian.Uint64(b[:8])
	if high != 0 {
		return bits.LeadingZeros64(high)
	}
	return 64 + bits.LeadingZeros64(binary.BigEndian.Uint64(a[8:])^binary.BigEndian.Uint64(b[8:]))
}
