package mmdbwriter

import (
	"io"
	"testing"
)

// First writes include expansion and canonicalization. Cached writes reuse
// final numbering, just as repeated WriteTo calls on an unchanged tree do.
func BenchmarkSubtreeWrite(b *testing.B) {
	b.Run("first", func(b *testing.B) {
		specs := overlappingBenchmarkInsertSpecs()
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			tree := newBenchmarkTree(b)
			insertBenchmarkSpecs(b, tree, specs)
			b.StartTimer()
			_, err := tree.WriteTo(io.Discard)
			requireNoBenchmarkError(b, err)
		}
	})
	b.Run("cached", func(b *testing.B) {
		tree := newBenchmarkTree(b)
		insertBenchmarkSpecs(b, tree, overlappingBenchmarkInsertSpecs())
		tree.finalize()
		b.ReportAllocs()
		for b.Loop() {
			_, err := tree.WriteTo(io.Discard)
			requireNoBenchmarkError(b, err)
		}
	})
}
