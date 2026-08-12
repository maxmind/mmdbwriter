package mmdbwriter

import (
	"errors"
	"fmt"
)

// RefcountAuditError reports that the reference-count audit found the tree's
// internal invariants violated. It signals a bug in this library or memory
// corruption, not a problem with the caller's input, so retrying the failed
// call cannot help.
type RefcountAuditError struct {
	err error
}

func (e *RefcountAuditError) Error() string { return e.err.Error() }

func (e *RefcountAuditError) Unwrap() error { return e.err }

// maybeAuditValueStore runs the ownership audit when the tree was created
// with Options.RefcountAudit or with MMDBWRITER_REFCOUNT_AUDIT set. Callers
// invoke it after an insert or load, once every temporary reference has been
// released. A failure comes back as a *RefcountAuditError, so callers can
// tell a broken tree from a rejected input.
func (t *Tree) maybeAuditValueStore() error {
	if !t.refcountAudit {
		return nil
	}
	if err := t.auditValueStore(); err != nil {
		return &RefcountAuditError{err: err}
	}
	return nil
}

// auditValueStore checks every ownership edge in the tree and value DAG. It
// is intentionally expensive. Production code reaches it through
// maybeAuditValueStore after successful inserts and loads. Tests call it
// directly.
func (t *Tree) auditValueStore() error {
	external := map[valueRef]uint64{}
	seenNodes := map[nodeIndex]bool{}
	seenPaths := map[nodeIndex]bool{}
	var walkRecord func(record) error
	var walkNode func(nodeIndex) error
	walkRecord = func(record record) error {
		switch record.recordType {
		case recordTypeData:
			external[record.value]++
		case recordTypeNode, recordTypeFixedNode:
			return walkNode(record.nodeIndex)
		case recordTypePath:
			if uint64(record.nodeIndex) >= uint64(len(t.paths)) {
				return fmt.Errorf("refcount audit found invalid path %d", record.nodeIndex)
			}
			if seenPaths[record.nodeIndex] {
				return fmt.Errorf(
					"refcount audit found path %d with multiple owning paths",
					record.nodeIndex,
				)
			}
			seenPaths[record.nodeIndex] = true
			return walkRecord(t.paths[record.nodeIndex].record)
		case recordTypeEmpty, recordTypeReserved, recordTypeAlias:
			return nil
		default:
			return fmt.Errorf("refcount audit found record type %d", record.recordType)
		}
		return nil
	}
	walkNode = func(index nodeIndex) error {
		if int64(index) >= int64(t.nodeCountAllocated) {
			return fmt.Errorf("refcount audit found invalid node %d", index)
		}
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
		if ref == nilValueRef || uint64(ref) >= uint64(len(s.nodes)) {
			return fmt.Errorf("refcount audit found invalid external ref %d", ref)
		}
		expected[ref] += count
	}
	if err := s.auditCallerIdentity(); err != nil {
		return err
	}
	if err := s.auditMaterializedIdentities(); err != nil {
		return err
	}
	if err := s.addCallerIdentityRefs(expected); err != nil {
		return err
	}
	bucketCounts := make([]int, len(s.nodes))
	for hash, head := range s.buckets {
		steps := 0
		for ref := head; ref != nilValueRef; ref = s.nodes[ref].nextInBucket {
			if uint64(ref) >= uint64(len(s.nodes)) {
				return fmt.Errorf("refcount audit found invalid bucket ref %d", ref)
			}
			if s.nodes[ref].kind == valueKindInvalid || s.nodes[ref].hash != hash {
				return fmt.Errorf("refcount audit found ref %d in the wrong hash bucket", ref)
			}
			bucketCounts[ref]++
			steps++
			if steps > len(s.nodes) {
				return fmt.Errorf("refcount audit found a bucket chain cycle for hash %d", hash)
			}
		}
	}
	freeCounts := make([]int, len(s.nodes))
	for _, ref := range s.freeRefs {
		if ref == nilValueRef || uint64(ref) >= uint64(len(s.nodes)) {
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
				uint64(child) >= uint64(len(s.nodes)) ||
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

// addCallerIdentityRefs adds the caller-identity cache's owned references to
// the expected counts. A ref outside the node arena is reported instead of
// indexed, so a corrupt entry produces a diagnostic rather than a panic.
func (s *valueStore) addCallerIdentityRefs(expected []uint64) error {
	for _, entry := range s.callerIdentity {
		if entry.ref == nilValueRef {
			continue
		}
		if uint64(entry.ref) >= uint64(len(s.nodes)) {
			return fmt.Errorf("refcount audit found invalid caller identity ref %d", entry.ref)
		}
		expected[entry.ref]++
	}
	return nil
}

// auditMaterializedIdentities validates the materialized identity map. Its
// entries are non-owning, so they add nothing to the expected counts. Each
// mapped ref must be live and carry the identity that maps to it.
func (s *valueStore) auditMaterializedIdentities() error {
	for identity, ref := range s.materializedByIdentity {
		if ref == nilValueRef || uint64(ref) >= uint64(len(s.nodes)) ||
			s.nodes[ref].kind == valueKindInvalid {
			return fmt.Errorf("identity audit found dead materialized ref %d", ref)
		}
		if !s.nodes[ref].hasIdentity || s.nodes[ref].identity != identity {
			return fmt.Errorf(
				"identity audit found ref %d under an identity it does not carry", ref)
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
