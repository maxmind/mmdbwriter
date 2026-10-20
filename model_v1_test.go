package mmdbwriter

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// v1Log records the operations of a model run that mmdbwriter v1 can
// replay, up to the first one it cannot. tools/v1replay reads the exported
// fields.
type v1Log struct {
	IPVersion       int    `json:"ipVersion"`
	DisableAliasing bool   `json:"disableAliasing"`
	IncludeReserved bool   `json:"includeReserved"`
	Inserter        string `json:"inserter"`
	Output          string `json:"output"`
	Ops             []v1Op `json:"ops"`

	started bool
	stopped bool
	// pending holds the model before an operation that v1 cannot replay.
	// If the operation leaves the model unchanged, the log continues.
	pending *treeModel
	// pre holds the model before the last logged operation, and lastErr its
	// v2 error.
	pre      *treeModel
	lastErr  error
	v2Errors []string
	traces   []string
	// model is the model when logging stopped.
	model *treeModel
	cfg   modelConfig
}

type v1Op struct {
	Kind   string `json:"kind"`
	Prefix string `json:"prefix,omitempty"`
	Start  string `json:"start,omitempty"`
	End    string `json:"end,omitempty"`
	Func   string `json:"func,omitempty"`
	Value  any    `json:"value"`
}

// v1Funcs are the test inserters that tools/v1replay implements.
var v1Funcs = map[string]bool{
	"Replace": true, "Remove": true, "TopLevelMerge": true, "DeepMerge": true,
	"keepOld": true, "copyOld": true, "wrapOld": true, "failOnU32_1": true,
	"failOnEmpty": true, "nilOnEqual": true,
}

func (l *v1Log) start(r *modelRun) bool {
	if l.stopped {
		return false
	}
	if !l.started {
		l.started = true
		l.IPVersion = r.opts.IPVersion
		l.DisableAliasing = r.opts.DisableIPv4Aliasing
		l.IncludeReserved = r.opts.IncludeReservedNetworks
		l.cfg = r.cfg
		if r.optsInserter != nil {
			if !v1Funcs[r.optsInserter.name] {
				l.stop(r)
				return false
			}
			l.Inserter = r.optsInserter.name
		}
	}
	return true
}

// stop ends the log and keeps the model as it stands.
func (l *v1Log) stop(r *modelRun) {
	if l.stopped {
		return
	}
	l.stopped = true
	l.model = r.model.clone()
	l.cfg = r.cfg
}

// skipped ends an operation that v1 cannot replay. If it changed the model,
// the log stops with the model from before it.
func (l *v1Log) skipped(r *modelRun) {
	before := l.pending
	l.pending = nil
	if before == nil || l.stopped {
		return
	}
	if !sameModel(before, r.model) {
		l.stopped = true
		l.model = before
		l.cfg = r.cfg
	}
}

// done ends a logged operation. After an inserter returns an error, v1
// leaves a broken record that makes WriteTo panic, even when nothing
// changed. So an operation that fails for any reason other than a reserved
// network, an alias, or an invalid range is removed, and the log stops
// before it.
func (l *v1Log) done(r *modelRun) {
	var reserved *ReservedNetworkError
	var aliased *AliasedNetworkError
	err := l.lastErr
	if err == nil || errors.As(err, &reserved) || errors.As(err, &aliased) ||
		strings.Contains(err.Error(), "did not give valid range") {
		return
	}
	n := len(l.Ops) - 1
	l.Ops, l.v2Errors, l.traces = l.Ops[:n], l.v2Errors[:n], l.traces[:n]
	l.stopped = true
	l.model = l.pre
	l.cfg = r.cfg
}

func sameModel(a, b *treeModel) bool {
	return slices.EqualFunc(a.segs, b.segs, func(x, y segment) bool {
		return x.start == y.start && x.key == y.key
	})
}

func (l *v1Log) result(trace string, err error) {
	l.lastErr = err
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	l.v2Errors = append(l.v2Errors, msg)
	l.traces = append(l.traces, trace)
}

// logValueAndFunc checks the parts that prefix and range inserts share.
func (l *v1Log) logValueAndFunc(r *modelRun, v mmdbtype.DataType, fn *namedPure) (any, string, bool) {
	// v1 merges +0.0 and -0.0, which the 2.0.0 changelog lists as fixed.
	if (v != nil && !validValue(v)) || hasNegativeZero(v) {
		return nil, "", false
	}
	name := ""
	if fn != nil {
		if !v1Funcs[fn.name] {
			return nil, "", false
		}
		name = fn.name
	}
	return encodeV1Value(normalizeValue(v)), name, true
}

func (l *v1Log) logPrefix(r *modelRun, p netip.Prefix, v mmdbtype.DataType, fn *namedPure) bool {
	if !l.start(r) {
		return false
	}
	value, name, ok := l.logValueAndFunc(r, v, fn)
	rng, valid := r.model.insertRange(p)
	if !ok || !valid {
		l.pending = r.model.clone()
		return false
	}
	bits := rangeDepth(rng, r.cfg.depth)
	text := ""
	switch {
	case r.cfg.depth == 32:
		text = netip.PrefixFrom(rng.first.addr4(), bits).String()
	case rng.first.hi == 0 && rng.first.lo>>32 == 0 && bits >= 96:
		text = netip.PrefixFrom(rng.first.addr4(), bits-96).String()
	default:
		text = netip.PrefixFrom(rng.first.addr16(), bits).String()
	}
	l.Ops = append(l.Ops, v1Op{Kind: "insert", Prefix: text, Func: name, Value: value})
	l.pre = r.model.clone()
	return true
}

func (l *v1Log) logRange(r *modelRun, start, end netip.Addr, v mmdbtype.DataType, fn *namedPure) bool {
	if !l.start(r) {
		return false
	}
	value, name, ok := l.logValueAndFunc(r, v, fn)
	s, e := start.Unmap(), end.Unmap()
	if !ok || !s.IsValid() || !e.IsValid() || s.Is4() != e.Is4() || (r.cfg.depth == 32 && !s.Is4()) {
		l.pending = r.model.clone()
		return false
	}
	l.Ops = append(l.Ops, v1Op{
		Kind: "insertRange", Start: s.String(), End: e.String(), Func: name, Value: value,
	})
	l.pre = r.model.clone()
	return true
}

// hasNegativeZero reports whether v holds a float -0.0.
func hasNegativeZero(v mmdbtype.DataType) bool {
	v, _ = derefValue(v)
	switch v := v.(type) {
	case mmdbtype.Float64:
		return math.Float64bits(float64(v)) == 1<<63
	case mmdbtype.Float32:
		return math.Float32bits(float32(v)) == 1<<31
	case mmdbtype.Map:
		for _, c := range v {
			if hasNegativeZero(c) {
				return true
			}
		}
	case mmdbtype.Slice:
		for _, c := range v {
			if hasNegativeZero(c) {
				return true
			}
		}
	}
	return false
}

// encodeV1Value writes a value in the typed JSON that tools/v1replay reads.
func encodeV1Value(v mmdbtype.DataType) any {
	switch v := v.(type) {
	case nil:
		return nil
	case mmdbtype.Map:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, string(k))
		}
		slices.Sort(keys)
		out := map[string]any{"t": "map", "k": []string{}, "s": []any{}}
		for _, k := range keys {
			out["k"] = append(out["k"].([]string), base64.StdEncoding.EncodeToString([]byte(k)))
			out["s"] = append(out["s"].([]any), encodeV1Value(v[mmdbtype.String(k)]))
		}
		return out
	case mmdbtype.Slice:
		children := []any{}
		for _, c := range v {
			children = append(children, encodeV1Value(c))
		}
		return map[string]any{"t": "slice", "s": children}
	case mmdbtype.String:
		return map[string]any{"t": "str", "b": base64.StdEncoding.EncodeToString([]byte(v))}
	case mmdbtype.Bytes:
		return map[string]any{"t": "bytes", "b": base64.StdEncoding.EncodeToString(v)}
	case mmdbtype.Bool:
		n := "0"
		if v {
			n = "1"
		}
		return map[string]any{"t": "bool", "n": n}
	case mmdbtype.Float64:
		return map[string]any{"t": "f64", "n": strconv.FormatUint(math.Float64bits(float64(v)), 16)}
	case mmdbtype.Float32:
		return map[string]any{"t": "f32", "n": strconv.FormatUint(uint64(math.Float32bits(float32(v))), 16)}
	case mmdbtype.Uint16:
		return map[string]any{"t": "u16", "n": strconv.FormatUint(uint64(v), 10)}
	case mmdbtype.Uint32:
		return map[string]any{"t": "u32", "n": strconv.FormatUint(uint64(v), 10)}
	case mmdbtype.Uint64:
		return map[string]any{"t": "u64", "n": strconv.FormatUint(uint64(v), 10)}
	case mmdbtype.Int32:
		return map[string]any{"t": "i32", "n": strconv.FormatInt(int64(v), 10)}
	case mmdbtype.Uint128:
		n := new(big.Int).Lsh(new(big.Int).SetUint64(v.High), 64)
		n.Or(n, new(big.Int).SetUint64(v.Low))
		return map[string]any{"t": "u128", "n": n.String()}
	}
	panic(fmt.Sprintf("cannot encode %T for v1", v))
}

// TestTreeModelV1Differential replays the operations of random model runs
// with mmdbwriter v1 and compares v1's errors and written database with the
// model, which matches v2. Set MMDBWRITER_MODEL_V1 to run it.
func TestTreeModelV1Differential(t *testing.T) {
	if os.Getenv("MMDBWRITER_MODEL_V1") == "" {
		t.Skip("set MMDBWRITER_MODEL_V1 to compare with mmdbwriter v1")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "v1replay")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Join("tools", "v1replay")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building v1replay: %v\n%s", err, out)
	}
	seed := uint64(1)
	if s := os.Getenv("MMDBWRITER_MODEL_SEED"); s != "" {
		var err error
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	runs := 300
	if s := os.Getenv("MMDBWRITER_MODEL_RUNS"); s != "" {
		var err error
		if runs, err = strconv.Atoi(s); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	differences := map[string][]string{}
	compared := 0
	for run := range runs {
		data := make([]byte, 32+rng.IntN(1500))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		log := &v1Log{}
		if err := runTreeModelWith(data, dir, modelRunOptions{v1: log}); err != nil {
			t.Fatal(err)
		}
		if len(log.Ops) == 0 {
			continue
		}
		compared++
		log.Output = filepath.Join(dir, fmt.Sprintf("v1-%d.mmdb", run))
		for kind, detail := range compareWithV1(t, bin, log) {
			differences[kind] = append(differences[kind], detail...)
		}
	}
	kinds := slices.Sorted(func(yield func(string) bool) {
		for k := range differences {
			if !yield(k) {
				return
			}
		}
	})
	for _, kind := range kinds {
		d := differences[kind]
		t.Errorf("%s: %d differences, for example:\n%s", kind, len(d),
			strings.Join(d[:min(3, len(d))], "\n---\n"))
	}
	t.Logf("compared %d runs", compared)
}

// compareWithV1 replays a log with v1 and returns the differences by kind.
func compareWithV1(t *testing.T, bin string, log *v1Log) map[string][]string {
	t.Helper()
	input, err := json.Marshal(log)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("v1replay: %v\n%s", err, stderr.String())
	}
	var res struct {
		Errors []string `json:"errors"`
		Fatal  string   `json:"fatal"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatal(err)
	}
	diffs := map[string][]string{}
	history := strings.Join(log.traces, "\n")
	if res.Fatal != "" {
		diffs["fatal"] = append(diffs["fatal"], res.Fatal+"\n"+history)
		return diffs
	}
	for i, v1err := range res.Errors {
		if (v1err == "") != (log.v2Errors[i] == "") {
			diffs["error"] = append(diffs["error"], fmt.Sprintf(
				"op %d: v1 error %q, v2 error %q\n%s", i, v1err, log.v2Errors[i], history))
			return diffs // The trees differ from here on.
		}
	}
	reader, err := maxminddb.Open(log.Output)
	if err != nil {
		diffs["open"] = append(diffs["open"], err.Error())
		return diffs
	}
	defer reader.Close()
	check := &modelRun{cfg: log.cfg, model: log.model}
	if err := check.checkNetworks(reader); err != nil {
		kind, _, _ := strings.Cut(err.Error(), "]")
		msg, _, _ := strings.Cut(err.Error(), "\ntrace:")
		diffs["value "+strings.TrimPrefix(kind, "[")] = append(
			diffs["value "+strings.TrimPrefix(kind, "[")], msg+"\n"+history)
	}
	return diffs
}
