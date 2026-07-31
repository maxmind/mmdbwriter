package mmdbwriter

import (
	"errors"
	"fmt"
)

// auditValueStore checks every ownership edge in the tree and value DAG. It is
// intentionally expensive and runs after successful inserts only when
// MMDBWRITER_REFCOUNT_AUDIT is set.
func (t *Tree) auditValueStore() error {
	external := map[valueRef]uint64{}
	seenNodes := map[nodeIndex]bool{}
	var walkRecord func(record) error
	var walkNode func(nodeIndex) error
	walkRecord = func(record record) error {
		switch record.recordType {
		case recordTypeData:
			external[record.value]++
		case recordTypeNode, recordTypeFixedNode:
			return walkNode(record.nodeIndex)
		case recordTypePath:
			return walkRecord(t.paths[record.nodeIndex].record)
		case recordTypeEmpty, recordTypeReserved, recordTypeAlias:
			return nil
		default:
			return fmt.Errorf("refcount audit found record type %d", record.recordType)
		}
		return nil
	}
	walkNode = func(index nodeIndex) error {
		if seenNodes[index] {
			return fmt.Errorf("refcount audit found node %d with multiple owning paths", index)
		}
		seenNodes[index] = true
		node := t.nodeAt(index)
		for _, child := range node.children {
			if err := walkRecord(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walkNode(t.root); err != nil {
		return err
	}
	return t.valueStore.audit(external)
}

func (s *valueStore) audit(external map[valueRef]uint64) error {
	expected := make([]uint64, len(s.nodes))
	for ref, count := range external {
		if ref == nilValueRef || int(ref) >= len(s.nodes) {
			return fmt.Errorf("refcount audit found invalid external ref %d", ref)
		}
		expected[ref] += count
	}
	if err := s.auditCallerIdentity(); err != nil {
		return err
	}
	for _, entry := range s.callerIdentity {
		if entry.ref != nilValueRef {
			expected[entry.ref]++
		}
	}
	bucketCounts := make([]int, len(s.nodes))
	for hash, bucket := range s.buckets {
		for _, ref := range bucket {
			if ref == nilValueRef || int(ref) >= len(s.nodes) {
				return fmt.Errorf("refcount audit found invalid bucket ref %d", ref)
			}
			if s.nodes[ref].kind == valueKindInvalid || s.nodes[ref].hash != hash {
				return fmt.Errorf("refcount audit found ref %d in the wrong hash bucket", ref)
			}
			bucketCounts[ref]++
		}
	}
	freeCounts := make([]int, len(s.nodes))
	for _, ref := range s.freeRefs {
		if ref == nilValueRef || int(ref) >= len(s.nodes) {
			return fmt.Errorf("refcount audit found invalid free ref %d", ref)
		}
		freeCounts[ref]++
	}
	for index := 1; index < len(s.nodes); index++ {
		node := &s.nodes[index]
		if node.kind == valueKindInvalid {
			if freeCounts[index] != 1 {
				return fmt.Errorf(
					"refcount audit found free ref %d listed %d times",
					index,
					freeCounts[index],
				)
			}
			continue
		}
		ref := valueRef(index)
		if bucketCounts[index] != 1 {
			return fmt.Errorf(
				"refcount audit found live ref %d in %d hash buckets",
				ref,
				bucketCounts[index],
			)
		}
		if freeCounts[index] != 0 {
			return fmt.Errorf("refcount audit found live ref %d on the freelist", ref)
		}
		for _, child := range s.childRefs(node) {
			if child == nilValueRef ||
				int(child) >= len(s.nodes) ||
				s.nodes[child].kind == valueKindInvalid {
				return fmt.Errorf("refcount audit found invalid child ref %d from %d", child, ref)
			}
			expected[child]++
		}
	}
	for index := 1; index < len(s.nodes); index++ {
		node := &s.nodes[index]
		if node.kind == valueKindInvalid {
			if expected[index] != 0 {
				return fmt.Errorf(
					"refcount audit expected %d references to free ref %d",
					expected[index],
					index,
				)
			}
			continue
		}
		if uint64(node.refCount) != expected[index] {
			return fmt.Errorf(
				"refcount audit for ref %d: stored %d, expected %d",
				index,
				node.refCount,
				expected[index],
			)
		}
	}
	return nil
}

func (s *valueStore) auditCallerIdentity() error {
	if len(s.callerIdentity) != len(s.callerByIdentity) {
		return fmt.Errorf(
			"caller identity audit has %d entries but %d indexes",
			len(s.callerIdentity),
			len(s.callerByIdentity),
		)
	}
	if len(s.callerIdentity) == 0 {
		if s.callerIdentityHead != -1 || s.callerIdentityTail != -1 {
			return errors.New("caller identity audit found a head or tail in an empty cache")
		}
		return nil
	}
	seen := make([]bool, len(s.callerIdentity))
	previous := -1
	count := 0
	for index := s.callerIdentityHead; index >= 0; index = s.callerIdentity[index].next {
		if index >= len(s.callerIdentity) || seen[index] {
			return fmt.Errorf("caller identity audit found an invalid LRU link at %d", index)
		}
		entry := s.callerIdentity[index]
		mappedIndex, ok := s.callerByIdentity[entry.key]
		if entry.prev != previous || !ok || mappedIndex != index {
			return fmt.Errorf("caller identity audit found an inconsistent entry at %d", index)
		}
		seen[index] = true
		previous = index
		count++
	}
	if count != len(s.callerIdentity) || previous != s.callerIdentityTail {
		return errors.New("caller identity audit found an incomplete LRU chain")
	}
	return nil
}
