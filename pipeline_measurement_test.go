package mmdbwriter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// TestPipelineMeasurement is an opt-in, single-process measurement harness.
// It loads MMDBWRITER_BENCHMARK_DB, merges the MMDBWRITER_PIPELINE_OVERLAYS
// databases in path-list order, then writes. The output hash detects output
// changes. Set MMDBWRITER_PIPELINE_METADATA to merge with InsertFunc.
// Compile once, then run fresh processes under /usr/bin/time -v with
// -test.run='^TestPipelineMeasurement$' -test.count=1 -test.v, so that no
// other test or repeat adds to the peak RSS.
func TestPipelineMeasurement(t *testing.T) {
	overlays := os.Getenv("MMDBWRITER_PIPELINE_OVERLAYS")
	if overlays == "" {
		t.Skip("MMDBWRITER_PIPELINE_OVERLAYS is not set")
	}
	path := os.Getenv("MMDBWRITER_BENCHMARK_DB")
	if path == "" {
		t.Fatal("MMDBWRITER_BENCHMARK_DB must be set when MMDBWRITER_PIPELINE_OVERLAYS is set")
	}
	// The audit checks the whole store after each insert, so it would
	// dominate every measurement.
	t.Setenv("MMDBWRITER_REFCOUNT_AUDIT", "")
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	tree, err := Load(path, Options{BuildEpoch: 123, IncludeReservedNetworks: true})
	require.NoError(t, err)
	reportPipelineStage(t, "load", path, 0, start, before)
	for _, overlay := range filepath.SplitList(overlays) {
		runtime.ReadMemStats(&before)
		start = time.Now()
		count := insertPipelineOverlay(t, tree, overlay)
		reportPipelineStage(t, "merge", overlay, count, start, before)
	}
	runtime.ReadMemStats(&before)
	start = time.Now()
	hash := sha256.New()
	written, err := tree.WriteTo(hash)
	require.NoError(t, err)
	reportPipelineStage(t, "write", path, 0, start, before)
	t.Logf("output_bytes=%d sha256=%s", written, hex.EncodeToString(hash.Sum(nil)))
	runtime.KeepAlive(tree)
}

func insertPipelineOverlay(t *testing.T, tree *Tree, path string) int {
	t.Helper()
	db, err := maxminddb.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	count := 0
	withMetadata := os.Getenv("MMDBWRITER_PIPELINE_METADATA") != ""
	for result := range db.Networks() {
		// Decode fresh input for each network. A shared Unmarshaler keeps
		// its offset cache, so it would retain the decoded overlay graph and
		// decode shared offsets only once.
		unmarshaler := mmdbtype.NewUnmarshaler()
		require.NoError(t, result.Decode(unmarshaler))
		if withMetadata {
			require.NoError(
				t,
				tree.InsertFunc(
					result.Prefix(),
					unmarshaler.Result(),
					func(existing, incoming mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
						return inserter.DeepMerge(existing, incoming)
					},
				),
			)
		} else {
			require.NoError(
				t,
				tree.InsertPureFunc(result.Prefix(), unmarshaler.Result(), inserter.DeepMerge),
			)
		}
		count++
	}
	return count
}

func reportPipelineStage(
	t *testing.T,
	stage, path string,
	count int,
	start time.Time,
	before runtime.MemStats,
) {
	t.Helper()
	elapsed := time.Since(start)
	// Collect garbage first so heap_bytes is the live heap.
	//revive:disable-next-line:call-to-gc
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	data, err := json.Marshal(map[string]any{
		"stage": stage, "database": filepath.Base(path), "networks": count,
		"ns": elapsed.Nanoseconds(), "alloc_bytes": after.TotalAlloc - before.TotalAlloc,
		"allocs": after.Mallocs - before.Mallocs, "heap_bytes": after.HeapAlloc,
	})
	require.NoError(t, err)
	t.Log(string(data))
}
