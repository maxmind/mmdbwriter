package mmdbwriter

import (
	"fmt"
	"io"
	"net/netip"
	"strconv"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func BenchmarkValueStoreChangingLayouts(b *testing.B) {
	for _, variableKey := range []int{8, 15} {
		b.Run(strconv.Itoa(variableKey), func(b *testing.B) {
			values := make([]mmdbtype.Map, 32)
			for version := range values {
				value := mmdbtype.Map{}
				for index := range 16 {
					key := fmt.Sprintf("key%03d", index)
					if index == variableKey {
						key += fmt.Sprintf("-%02d", version)
					}
					value[mmdbtype.String(key)] = mmdbtype.Uint32(index)
				}
				values[version] = value
			}
			store := newValueStore()
			var previous valueRef
			index := 0
			b.ReportAllocs()
			for b.Loop() {
				ref, err := store.intern(values[index%len(values)])
				requireNoBenchmarkError(b, err)
				store.release(previous)
				previous = ref
				index++
			}
			store.release(previous)
		})
	}
}

func BenchmarkTreeGrowingCallbackValues(b *testing.B) {
	const count = 2048
	values := make([]mmdbtype.Slice, count)
	for index := range values {
		values[index] = make(mmdbtype.Slice, index+1)
		for child := range values[index] {
			values[index][child] = mmdbtype.Bool(true)
		}
	}
	var tree *Tree
	prefix := netip.MustParsePrefix("1.0.0.0/8")
	b.ReportAllocs()
	for b.Loop() {
		tree = newBenchmarkTree(b)
		for _, value := range values {
			requireNoBenchmarkError(b, tree.InsertPureFunc(prefix, value, inserter.Replace))
		}
	}
	b.ReportMetric(float64(len(tree.valueStore.children.data)*4), "retained_child_bytes/op")
}

func BenchmarkSparseIPv6Pipeline(b *testing.B) {
	for _, write := range []bool{false, true} {
		b.Run(fmt.Sprintf("write=%v", write), func(b *testing.B) {
			var tree *Tree
			b.ReportAllocs()
			for b.Loop() {
				tree = subtreeSyntheticTree(b, false)
				if write {
					_, err := tree.WriteTo(io.Discard)
					requireNoBenchmarkError(b, err)
				}
			}
			b.ReportMetric(float64(tree.nodeCountAllocated), "allocated_nodes/op")
		})
	}
}
