package mmdbwriter

import "fmt"

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
	for _, entry := range s.callerIdentity {
		if entry.ref != nilValueRef {
			expected[entry.ref]++
		}
	}
	for index := 1; index < len(s.nodes); index++ {
		node := &s.nodes[index]
		if node.kind == valueKindInvalid {
			continue
		}
		ref := valueRef(index)
		found := false
		for _, candidate := range s.buckets[node.hash] {
			if candidate == ref {
				if found {
					return fmt.Errorf("refcount audit found duplicate bucket ref %d", ref)
				}
				found = true
			}
		}
		if !found {
			return fmt.Errorf("refcount audit found ref %d missing from hash bucket", ref)
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
