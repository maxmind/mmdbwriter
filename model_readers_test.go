package mmdbwriter

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// readerSnapshot is a database that a model run wrote, with the model as it
// stood then and the addresses to look up in it.
type readerSnapshot struct {
	file    string
	model   *treeModel
	queries []netip.Addr
}

type pythonResult struct {
	File     string  `json:"file"`
	Mode     string  `json:"mode"`
	Lookups  [][]any `json:"lookups"`
	Networks [][]any `json:"networks"`
	Error    string  `json:"error"`
}

// TestTreeModelPythonReaders reads databases from random model runs with the
// Python maxminddb package, in its libmaxminddb C extension mode and its
// pure-Python mode, and compares lookups and network iteration with the
// model. MMDBWRITER_MODEL_PYTHON names a Python interpreter that has the
// maxminddb package.
func TestTreeModelPythonReaders(t *testing.T) {
	python := os.Getenv("MMDBWRITER_MODEL_PYTHON")
	if python == "" {
		t.Skip("set MMDBWRITER_MODEL_PYTHON to a Python with the maxminddb package")
	}
	seed := uint64(1)
	if s := os.Getenv("MMDBWRITER_MODEL_SEED"); s != "" {
		var err error
		seed, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	runs := 100
	if s := os.Getenv("MMDBWRITER_MODEL_RUNS"); s != "" {
		var err error
		runs, err = strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	var snapshots []readerSnapshot
	onWrite := func(r *modelRun, db []byte) {
		if r.modelHasInvalidUTF8() {
			return // Known issue: the writer accepts strings that are not UTF-8.
		}
		file := filepath.Join(dir, fmt.Sprintf("db-%d.mmdb", len(snapshots)))
		if err := os.WriteFile(file, db, 0o600); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, readerSnapshot{
			file:    file,
			model:   r.model.clone(),
			queries: readerQueries(r),
		})
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	for range runs {
		data := make([]byte, 32+rng.IntN(1500))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		if err := runTreeModelWith(data, dir, false, onWrite); err != nil {
			t.Fatal(err)
		}
	}

	results := runPythonReaders(t, python, snapshots)
	byFile := map[string]readerSnapshot{}
	for _, snap := range snapshots {
		byFile[snap.file] = snap
	}
	failures := map[string][]string{}
	for _, res := range results {
		skipKnown := os.Getenv("MMDBWRITER_MODEL_KNOWN") != "off"
		for _, problem := range checkPythonResult(byFile[res.File], res, skipKnown) {
			kind, _, _ := strings.Cut(problem, ":")
			key := res.Mode + " " + kind
			failures[key] = append(failures[key], problem)
		}
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range failures {
			if !yield(k) {
				return
			}
		}
	})
	for _, key := range keys {
		t.Errorf("%s: %d problems, for example:\n  %s", key, len(failures[key]),
			strings.Join(failures[key][:min(3, len(failures[key]))], "\n  "))
	}
	t.Logf("checked %d databases", len(snapshots))
}

// knownPythonError reports whether an iteration error comes from a known bug
// in the Python reader:
//
//   - The pure-Python reader treats the address 2^32 (::1:0:0) as IPv4,
//     because _IPV4_MAX_NUM is 2**32, not 2**32 - 1.
//   - Both modes subtract 96 from the depth of every record on the zero path,
//     even above /96, which gives a negative prefix length.
func knownPythonError(msg string) bool {
	return (strings.Contains(msg, "::1:0:0/") && strings.Contains(msg, "has host bits set")) ||
		(strings.Contains(msg, "does not appear to be an IPv4 or IPv6 network") &&
			strings.Contains(msg, ", -"))
}

// readerQueries returns the addresses to look up: the probes and anchors, in
// every form a reader accepts.
func readerQueries(r *modelRun) []netip.Addr {
	var out []netip.Addr
	for _, a := range append(slices.Clone(r.probes), r.anchors...) {
		if r.cfg.depth == 32 {
			out = append(out, a.addr4())
			continue
		}
		out = append(out, a.addr16())
		if a.hi == 0 && a.lo>>32 == 0 {
			out = append(out, a.addr4(), netip.AddrFrom16(a.addr4().As16()))
		}
	}
	return out
}

func runPythonReaders(t *testing.T, python string, snapshots []readerSnapshot) []pythonResult {
	t.Helper()
	type entry struct {
		File    string   `json:"file"`
		Queries []string `json:"queries"`
	}
	manifest := make([]entry, 0, len(snapshots))
	for _, snap := range snapshots {
		e := entry{File: snap.file}
		for _, q := range snap.queries {
			e.Queries = append(e.Queries, q.String())
		}
		manifest = append(manifest, e)
	}
	input, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join("testdata", "read-with-python.py"))
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("running the Python readers: %v\n%s", err, stderr.String())
	}
	var results []pythonResult
	for line := range strings.Lines(string(output)) {
		var res pythonResult
		if err := json.Unmarshal([]byte(line), &res); err != nil {
			t.Fatal(err)
		}
		results = append(results, res)
	}
	return results
}

// checkPythonResult returns one "kind: detail" string for each problem.
func checkPythonResult(snap readerSnapshot, res pythonResult, skipKnown bool) []string {
	m := snap.model
	var problems []string
	if res.Error != "" && !(skipKnown && knownPythonError(res.Error)) {
		problems = append(problems, fmt.Sprintf("error: %s: %s", filepath.Base(res.File), res.Error))
	}
	for _, l := range res.Lookups {
		addr := netip.MustParseAddr(l[0].(string))
		prefixLen := int(l[1].(float64))
		got := l[2].(string)
		ma, _ := m.readerAddr(addr)
		want := looseCanon(m.get(m.resolveAlias(ma)).value)
		if got != want {
			problems = append(problems, fmt.Sprintf("lookup-value: %s: %s = %s, model %s",
				filepath.Base(res.File), addr, got, want))
			continue
		}
		prefix, err := addr.Prefix(prefixLen)
		if err != nil {
			problems = append(problems, fmt.Sprintf("lookup-prefix: %s: %s has prefix length %d",
				filepath.Base(res.File), addr, prefixLen))
			continue
		}
		pr := m.prefixToRange(prefix)
		resolved, ok := m.resolveAliasRange(pr)
		seg, constant := m.constant(resolved)
		if !pr.contains(ma) || !ok || !constant || looseCanon(seg.value) != want {
			problems = append(problems, fmt.Sprintf("lookup-prefix: %s: %s in %s, not constant in the model",
				filepath.Base(res.File), addr, prefix))
		}
	}
	if res.Error == "" {
		// A failed iteration stops early, so its missing networks say
		// nothing more.
		problems = append(problems, checkPythonNetworks(snap, res, skipKnown)...)
	}
	return problems
}

// checkPythonNetworks checks that the iteration yields disjoint networks
// that each hold one model value, and that they cover every address outside
// the aliases that holds a value.
func checkPythonNetworks(snap readerSnapshot, res pythonResult, skipKnown bool) []string {
	m := snap.model
	name := filepath.Base(res.File)
	var problems []string
	covered := newTreeModel(modelConfig{depth: m.cfg.depth})
	var ranges []mrange
	for _, n := range res.Networks {
		prefix, err := netip.ParsePrefix(n[0].(string))
		if err != nil {
			problems = append(problems, fmt.Sprintf("network-parse: %s: %v", name, err))
			continue
		}
		nr := m.prefixToRange(prefix)
		seg, ok := m.constant(nr)
		if !ok || looseCanon(seg.value) != n[1].(string) {
			problems = append(problems, fmt.Sprintf("network-value: %s: %s holds %s, model constant=%v %s",
				name, prefix, n[1], ok, looseCanon(seg.value)))
		}
		ranges = append(ranges, nr)
		covered.set(nr, mmdbtype.Bool(true))
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].first.cmp(ranges[j].first) < 0 })
	for i := 1; i < len(ranges); i++ {
		if ranges[i].first.cmp(ranges[i-1].last) <= 0 {
			problems = append(problems, fmt.Sprintf("network-overlap: %s: %s overlaps the previous network",
				name, ranges[i].first.addr16()))
		}
	}
	whole := mrange{u128{}, lowMask(m.cfg.depth)}
	for _, part := range subtractRanges(whole, aliasRanges(m.aliases)) {
		for _, run := range m.runs(part) {
			if m.get(run.first).value == nil {
				continue
			}
			// Known bug: like maxminddb-golang, the pure-Python iteration
			// skips networks that share the IPv4 entry's data record.
			if skipKnown && res.Mode == "pure-python" && m.cfg.depth == 128 && !m.cfg.aliasing &&
				m.get(run.first).key == m.get(u128{}).key {
				continue
			}
			if seg, ok := covered.constant(run); !ok || seg.value == nil {
				problems = append(problems, fmt.Sprintf("network-missing: %s: %s-%s holds %s",
					name, run.first.addr16(), run.last.addr16(), looseCanon(m.get(run.first).value)))
			}
		}
	}
	return problems
}

// looseCanon encodes a value the way testdata/read-with-python.py does.
// Integers lose their MMDB type, and float32 values become float64 values.
func looseCanon(v mmdbtype.DataType) string {
	var b strings.Builder
	writeLooseCanon(&b, v)
	return b.String()
}

func writeLooseCanon(b *strings.Builder, v mmdbtype.DataType) {
	v, _ = derefValue(v)
	switch v := v.(type) {
	case nil:
		b.WriteString("nil")
	case mmdbtype.Bool:
		fmt.Fprintf(b, "bool(%t)", bool(v))
	case mmdbtype.Uint16:
		fmt.Fprintf(b, "int(%d)", v)
	case mmdbtype.Uint32:
		fmt.Fprintf(b, "int(%d)", v)
	case mmdbtype.Uint64:
		fmt.Fprintf(b, "int(%d)", v)
	case mmdbtype.Int32:
		fmt.Fprintf(b, "int(%d)", v)
	case mmdbtype.Uint128:
		n := new(big.Int).Lsh(new(big.Int).SetUint64(v.High), 64)
		n.Or(n, new(big.Int).SetUint64(v.Low))
		fmt.Fprintf(b, "int(%s)", n)
	case mmdbtype.Float64:
		fmt.Fprintf(b, "f(%016x)", math.Float64bits(float64(v)))
	case mmdbtype.Float32:
		fmt.Fprintf(b, "f(%016x)", math.Float64bits(float64(float32(v))))
	case mmdbtype.String:
		fmt.Fprintf(b, "str(%s)", hex.EncodeToString([]byte(v)))
	case mmdbtype.Bytes:
		fmt.Fprintf(b, "bytes(%s)", hex.EncodeToString(v))
	case mmdbtype.Map:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, string(k))
		}
		slices.Sort(keys)
		b.WriteString("map{")
		for _, k := range keys {
			fmt.Fprintf(b, "%s:", hex.EncodeToString([]byte(k)))
			writeLooseCanon(b, v[mmdbtype.String(k)])
			b.WriteString(",")
		}
		b.WriteString("}")
	case mmdbtype.Slice:
		b.WriteString("list[")
		for _, e := range v {
			writeLooseCanon(b, e)
			b.WriteString(",")
		}
		b.WriteString("]")
	default:
		fmt.Fprintf(b, "unknown(%T)", v)
	}
}
