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

// TestPipelineMeasurement runs once per process so /usr/bin/time can measure
// peak RSS without retaining warm-up trees. Optional overlays are applied in
// path-list order, followed by a write whose hash detects output changes.
func TestPipelineMeasurement(t *testing.T) {
	path := os.Getenv("MMDBWRITER_PIPELINE_DB")
	if path == "" {
		t.Skip("MMDBWRITER_PIPELINE_DB is not set")
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	tree, err := Load(path, Options{
		BuildEpoch:              123,
		IncludeReservedNetworks: true,
		RecordSize:              32,
	})
	require.NoError(t, err)
	reportPipelineStage(t, "load", path, 0, start, before)
	for _, overlay := range filepath.SplitList(os.Getenv("MMDBWRITER_PIPELINE_OVERLAYS")) {
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
	defer db.Close()
	unmarshaler := mmdbtype.NewUnmarshaler()
	count := 0
	withMetadata := os.Getenv("MMDBWRITER_PIPELINE_METADATA") != ""
	for result := range db.Networks() {
		unmarshaler.Clear()
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
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	data, err := json.Marshal(map[string]any{
		"stage": stage, "database": filepath.Base(path), "networks": count,
		"ns": elapsed.Nanoseconds(), "allocated_bytes": after.TotalAlloc - before.TotalAlloc,
		"allocs": after.Mallocs - before.Mallocs, "heap_bytes": after.HeapAlloc,
	})
	require.NoError(t, err)
	t.Log(string(data))
}
