package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"net/netip"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// byteSource turns fuzz input or PRNG output into choices. It returns zero
// after the input is used up, and the run stops at the next operation.
type byteSource struct {
	data []byte
	pos  int
}

func (s *byteSource) exhausted() bool { return s.pos >= len(s.data) }

func (s *byteSource) byte() byte {
	if s.pos >= len(s.data) {
		s.pos++
		return 0
	}
	b := s.data[s.pos]
	s.pos++
	return b
}

func (s *byteSource) intn(n int) int {
	if n <= 256 {
		return int(s.byte()) % n
	}
	return (int(s.byte())<<8 | int(s.byte())) % n
}

type namedPure struct {
	name string
	fn   inserter.PureFunc
	// model is the function the model calls. It is a reference version for
	// the inserter package's merges, and fn itself for test functions.
	model inserter.PureFunc
}

func (f namedPure) modelFunc() inserter.PureFunc {
	if f.model != nil {
		return f.model
	}
	return f.fn
}

// modelPureFuncs are all pure, so the model can call them with its own copy
// of the existing value and expect the same result as the tree.
var modelPureFuncs = []namedPure{
	{name: "Replace", fn: inserter.Replace},
	{name: "Remove", fn: inserter.Remove},
	{name: "TopLevelMerge", fn: inserter.TopLevelMerge, model: referenceTopLevelMerge},
	{name: "DeepMerge", fn: inserter.DeepMerge, model: referenceDeepMerge},
	{name: "keepOld", fn: func(old, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
		return old, nil
	}},
	{name: "copyOld", fn: func(old, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
		if old == nil {
			return nil, nil
		}
		return old.Copy(), nil
	}},
	{name: "wrapOld", fn: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
		if newValue == nil {
			return nil, nil
		}
		if old == nil {
			old = mmdbtype.String("<nil>")
		}
		return mmdbtype.Map{"old": old, "new": newValue}, nil
	}},
	{name: "failOnU32_1", fn: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
		if canon(old) == canon(mmdbtype.Uint32(1)) {
			return nil, errModelCallback
		}
		return newValue, nil
	}},
	{name: "failOnEmpty", fn: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
		if old == nil {
			return nil, errModelCallback
		}
		return newValue, nil
	}},
	{name: "invalidOnEmpty", fn: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
		if old == nil {
			return mmdbtype.Map{"x": nil}, nil
		}
		return newValue, nil
	}},
	{name: "nilOnEqual", fn: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
		if old != nil && newValue != nil && canon(old) == canon(newValue) {
			return nil, nil
		}
		return newValue, nil
	}},
	{
		name: "panicOnU32_1",
		fn: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
			if canon(old) == canon(mmdbtype.Uint32(1)) {
				panic(errModelPanic)
			}
			return newValue, nil
		},
		// The model stops where the tree panics.
		model: func(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
			if canon(old) == canon(mmdbtype.Uint32(1)) {
				return nil, errModelPanic
			}
			return newValue, nil
		},
	},
}

// errModelPanic is the value a test inserter panics with.
var errModelPanic = errors.New("model test inserter panic")

// callTree runs a tree operation and recovers a panic from a test inserter.
// Any other panic propagates.
func callTree(op func() error) (err error, panicked bool) {
	defer func() {
		if p := recover(); p != nil {
			if p != errModelPanic { //nolint:errorlint // The panic value is the sentinel itself.
				panic(p)
			}
			panicked = true
		}
	}()
	return op(), false
}

// referenceTopLevelMerge states TopLevelMerge's documented behavior without
// sharing its code.
func referenceTopLevelMerge(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
	n, ok := newValue.(mmdbtype.Map)
	if !ok {
		return nil, errors.New("new value is not a Map")
	}
	if old == nil {
		return newValue, nil
	}
	o, ok := old.(mmdbtype.Map)
	if !ok {
		return nil, errors.New("existing value is not a Map")
	}
	out := mmdbtype.Map{}
	for k, v := range o {
		out[k] = v
	}
	for k, v := range n {
		out[k] = v
	}
	return out, nil
}

// referenceDeepMerge states DeepMerge's documented behavior without sharing
// its code: maps merge by key and slices by index, recursively, and any other
// new value replaces the existing one.
func referenceDeepMerge(old, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
	switch {
	case old == nil:
		return newValue, nil
	case newValue == nil:
		return old, nil
	}
	switch o := old.(type) {
	case mmdbtype.Map:
		n, ok := newValue.(mmdbtype.Map)
		if !ok {
			return newValue, nil
		}
		out := mmdbtype.Map{}
		for k, v := range o {
			out[k] = v
		}
		for k, v := range n {
			merged, err := referenceDeepMerge(o[k], v)
			if err != nil {
				return nil, err
			}
			out[k] = merged
		}
		return out, nil
	case mmdbtype.Slice:
		n, ok := newValue.(mmdbtype.Slice)
		if !ok {
			return newValue, nil
		}
		out := make(mmdbtype.Slice, max(len(o), len(n)))
		for i := range out {
			var ov, nv mmdbtype.DataType
			if i < len(o) {
				ov = o[i]
			}
			if i < len(n) {
				nv = n[i]
			}
			merged, err := referenceDeepMerge(ov, nv)
			if err != nil {
				return nil, err
			}
			out[i] = merged
		}
		return out, nil
	}
	return newValue, nil
}

func modelValuePool() []mmdbtype.DataType {
	s := func(n int) mmdbtype.String { return mmdbtype.String(strings.Repeat("s", n)) }
	big := mmdbtype.Map{}
	for i := range 70 {
		big[mmdbtype.String(fmt.Sprintf("k%02d", i))] = mmdbtype.Uint32(i)
	}
	ptrString := mmdbtype.String("p")
	ptrU32 := mmdbtype.Uint32(1)
	return []mmdbtype.DataType{
		// 0-7: common values, so equal siblings and merges are frequent.
		nil,
		mmdbtype.Uint32(1),
		mmdbtype.Uint32(2),
		mmdbtype.Map{"a": mmdbtype.Uint32(1)},
		mmdbtype.Map{"a": mmdbtype.Uint32(1)}, // Equal content, distinct object.
		mmdbtype.Map{"a": mmdbtype.Uint32(2)},
		mmdbtype.String("a"),
		mmdbtype.Map{"b": mmdbtype.Uint32(1)},
		// 8-: rarer valid values.
		mmdbtype.Uint16(1),
		mmdbtype.Uint64(1),
		mmdbtype.String(""),
		mmdbtype.Float64(0),
		mmdbtype.Float64(math.Copysign(0, -1)),
		mmdbtype.Float64(math.Float64frombits(0x7ff8000000000001)),
		mmdbtype.Float64(math.Float64frombits(0x7ff8000000000002)),
		mmdbtype.Float32(0),
		mmdbtype.Float32(float32(math.Copysign(0, -1))),
		mmdbtype.Bool(true),
		mmdbtype.Bool(false),
		mmdbtype.Int32(-1),
		mmdbtype.Uint128{High: 1, Low: 2},
		mmdbtype.Bytes{},
		mmdbtype.Bytes{0},
		mmdbtype.Map{},
		mmdbtype.Slice{},
		mmdbtype.Map{"a": mmdbtype.Uint32(1), "b": mmdbtype.Uint32(2)},
		mmdbtype.Map{"a": mmdbtype.Uint32(1), "c": mmdbtype.Uint32(2)},
		mmdbtype.Map{"a": mmdbtype.Uint32(1), "b": mmdbtype.Slice{
			mmdbtype.String("x"), mmdbtype.Float64(math.Copysign(0, -1)),
		}},
		mmdbtype.Map{"a": mmdbtype.Map{"b": mmdbtype.Map{"c": mmdbtype.Uint32(1)}}},
		mmdbtype.Map{"a": mmdbtype.Map{"b": mmdbtype.Map{"c": mmdbtype.Uint32(2)}}},
		mmdbtype.Map{"a": mmdbtype.Map{"b": mmdbtype.Map{"d": mmdbtype.Uint32(1)}}},
		mmdbtype.Slice{mmdbtype.Uint32(1), mmdbtype.Uint32(2)},
		mmdbtype.Slice{mmdbtype.Map{"a": mmdbtype.Uint32(1)}},
		s(28), s(29), s(284), s(285),
		// Load interns map keys through a separate encoder.
		mmdbtype.Map{s(28): mmdbtype.Uint32(1), s(29): mmdbtype.Uint32(1)},
		mmdbtype.Map{s(284): mmdbtype.Uint32(1), s(285): mmdbtype.Uint32(1)},
		mmdbtype.Bytes(bytes.Repeat([]byte{7}, 29)),
		big,
		&ptrString,
		mmdbtype.Map{"a": &ptrU32},
		// Invalid values. Keep these last, see genValue.
		mmdbtype.Map{"x": nil},
		mmdbtype.Slice{nil},
		mmdbtype.Pointer(1),
		(*mmdbtype.Map)(nil),
		mmdbtype.Map{"a": mmdbtype.Slice{mmdbtype.Pointer(2)}},
	}
}

const modelInvalidValues = 5

type modelRun struct {
	src  *byteSource
	dir  string
	opts Options
	// optsInserter is the named form of opts.Inserter.
	optsInserter *namedPure
	cfg          modelConfig
	tree         *Tree
	model        *treeModel
	values       []mmdbtype.DataType
	views        []mmdbtype.DataType
	// anchors are tree-space addresses that generated addresses cluster
	// around, so generated prefixes nest, overlap, and touch.
	anchors []u128
	// probes are tree-space addresses touched by recent operations.
	probes []u128
	trace  []string
	loads  int
	// strictGetReader also requires Tree.Get and a reader lookup of the same
	// address to agree.
	strictGetReader bool
	// callbackErr holds the first failure a callback found. Callbacks cannot
	// return it, so the insert check reports it.
	callbackErr error
	// onWrite, if set, receives every database that fullCheck writes.
	onWrite func(*modelRun, []byte)
	// v1, if set, logs the operations that mmdbwriter v1 can replay.
	v1 *v1Log
	// skipKnown skips checks that fail because of bugs this prototype has
	// already found, so a run can find other bugs.
	skipKnown bool
}

const modelMaxOperations = 64

// runTreeModel interprets data as a tree configuration and a sequence of
// operations. It applies each operation to a Tree and to the model and
// returns the first disagreement.
func runTreeModel(data []byte, dir string, strict bool) error {
	return runTreeModelWith(data, dir, modelRunOptions{strict: strict})
}

// modelRunOptions holds the optional parts of a model run.
type modelRunOptions struct {
	// strict also requires Tree.Get and a reader lookup to agree.
	strict bool
	// onWrite, if set, receives every database that the full check writes.
	onWrite func(*modelRun, []byte)
	// v1, if set, receives the operations that mmdbwriter v1 can replay.
	v1 *v1Log
}

// runTreeModelWith is runTreeModel with optional parts.
func runTreeModelWith(data []byte, dir string, opts modelRunOptions) (err error) {
	r := &modelRun{
		src:             &byteSource{data: data},
		dir:             dir,
		strictGetReader: opts.strict,
		skipKnown:       os.Getenv("MMDBWRITER_MODEL_KNOWN") != "off",
		onWrite:         opts.onWrite,
		v1:              opts.v1,
	}
	if r.v1 != nil {
		defer r.v1.stop(r)
	}
	defer func() {
		if p := recover(); p != nil {
			err = r.fail("panic", "%v\n%s", p, debug.Stack())
		}
	}()
	if err := r.setup(); err != nil {
		return err
	}
	for i := 0; i < modelMaxOperations && !r.src.exhausted(); i++ {
		if err := r.step(i); err != nil {
			return err
		}
		if i%8 == 7 {
			if err := r.fullCheck(); err != nil {
				return err
			}
		}
	}
	return r.fullCheck()
}

func (r *modelRun) fail(tag, format string, args ...any) error {
	cursor := r.tree != nil && !r.tree.disableInsertCursor
	return fmt.Errorf("[%s] %s\nconfig: %+v cursor=%v audit=%v\ntrace:\n%s",
		tag, fmt.Sprintf(format, args...), r.cfg,
		cursor, r.opts.RefcountAudit,
		strings.Join(r.trace, "\n"))
}

func (r *modelRun) setup() error {
	b := r.src.byte()
	r.opts = Options{
		BuildEpoch:              1,
		DatabaseType:            "model-test",
		Description:             map[string]string{"en": "Model test"},
		IPVersion:               6,
		DisableIPv4Aliasing:     b&2 != 0,
		IncludeReservedNetworks: b&4 != 0,
		RefcountAudit:           b&8 != 0,
		DisableMetadataPointers: b&128 != 0,
		RecordSize:              []int{0, 24, 28, 32}[r.src.intn(4)],
	}
	if b&1 != 0 {
		r.opts.IPVersion = 4
	}
	if choice := (b >> 5) & 3; choice != 0 {
		f := modelPureFuncs[[]int{0, 0, 3, 6}[choice]] // Replace, DeepMerge, wrapOld
		r.optsInserter = &f
		r.opts.Inserter = f.fn
	}
	r.cfg = configFor(r.opts)
	tree, err := New(r.opts)
	if err != nil {
		return fmt.Errorf("New: %w", err)
	}
	tree.disableInsertCursor = b&16 != 0
	r.tree = tree
	r.model = newTreeModel(r.cfg)
	r.values = modelValuePool()
	r.anchors = modelAnchors(r.cfg)
	r.trace = append(r.trace, fmt.Sprintf("New(%s)", r.describeOptions()))
	return nil
}

// configFor returns the model configuration for tree options with an
// explicit IPVersion.
func configFor(opts Options) modelConfig {
	if opts.IPVersion == 4 {
		return modelConfig{depth: 32, reserved: !opts.IncludeReservedNetworks}
	}
	return modelConfig{
		depth:    128,
		aliasing: !opts.DisableIPv4Aliasing,
		reserved: !opts.IncludeReservedNetworks,
	}
}

func (r *modelRun) describeOptions() string {
	name := "nil"
	if r.optsInserter != nil {
		name = r.optsInserter.name
	}
	return fmt.Sprintf(
		"IPVersion:%d DisableIPv4Aliasing:%v IncludeReservedNetworks:%v RecordSize:%d Inserter:%s",
		r.opts.IPVersion, r.opts.DisableIPv4Aliasing, r.opts.IncludeReservedNetworks,
		r.opts.RecordSize, name,
	)
}

func modelAnchors(cfg modelConfig) []u128 {
	v4 := []string{
		"1.2.3.0", "1.2.3.255", "9.255.255.255", "10.0.0.0", "11.0.0.0",
		"223.255.255.255", "224.0.0.0", "0.0.0.0", "255.255.255.255",
		"127.255.255.255", "128.0.0.0", "192.0.2.0", "100.63.255.255",
	}
	var out []u128
	for _, s := range v4 {
		out = append(out, u128FromAddr4(netip.MustParseAddr(s)))
	}
	if cfg.depth == 32 {
		return out
	}
	v6 := []string{
		"::", "::1", "::fffe:ffff:ffff", "::ffff:0:0", "::ffff:1.2.3.4",
		"::1:0:0:0", "64:ff9b::102:304", "2001::", "2001:0:102:304::",
		"2001:1::", "2001:db8::", "2002::", "2002:102:304::", "2003::",
		"fe80::", "ff00::", "8000::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
	}
	for _, s := range v6 {
		out = append(out, u128FromAddr16(netip.MustParseAddr(s)))
	}
	// IPv4 suffixes under another /96 build subtrees equal to the IPv4
	// subtree, which the writer may share.
	for _, s := range []string{"1.2.3.0", "10.0.0.0"} {
		out = append(out, u128{0x2003 << 48, u128FromAddr4(netip.MustParseAddr(s)).lo})
	}
	return out
}

func (r *modelRun) genModelAddr() u128 {
	a := r.anchors[r.src.intn(len(r.anchors))]
	switch mode := r.src.byte(); {
	case mode < 128:
		bits := int(mode % 20)
		noise := u128{uint64(r.src.byte()) << 56, uint64(r.src.byte())<<8 | uint64(r.src.byte())}
		a = u128{a.hi ^ noise.hi&lowMask(bits).hi, a.lo ^ noise.lo&lowMask(bits).lo}
	case mode < 192:
		for range int(mode % 8) {
			a = a.add1()
		}
	case mode < 224:
		for range int(mode % 8) {
			a = a.sub1()
		}
	}
	return a.and(lowMask(r.cfg.depth))
}

func (r *modelRun) genBits() int {
	d := r.cfg.depth
	switch c := r.src.byte(); {
	case c < 160:
		return d - int(c%17)
	case c < 224:
		return int(c) % (d + 1)
	default:
		return []int{0, 1, 8, 16, 31, 32, 95, 96, 97}[int(c)%9] % (d + 1)
	}
}

// apiAddr returns one of the forms a caller can use for a tree-space address.
func (r *modelRun) apiAddr(a u128) netip.Addr {
	form := r.src.byte()
	if r.cfg.depth == 32 {
		switch {
		case form < 224:
			return a.addr4()
		case form < 248:
			return netip.AddrFrom16(a.addr4().As16())
		}
		return a.addr16() // An IPv6 address, which an IPv4 tree rejects.
	}
	if a.hi == 0 && a.lo>>32 == 0 {
		switch {
		case form < 160:
			return a.addr4()
		case form < 208:
			return netip.AddrFrom16(a.addr4().As16()) // IPv4-mapped
		}
	}
	return a.addr16()
}

func (r *modelRun) genPrefix() netip.Prefix {
	if r.src.byte() == 255 {
		return netip.Prefix{}
	}
	return r.apiPrefix(r.genModelAddr(), r.genBits())
}

// apiPrefix returns one of the forms a caller can use for a tree-space
// prefix.
func (r *modelRun) apiPrefix(a u128, bits int) netip.Prefix {
	addr := r.apiAddr(a)
	switch {
	case addr.Is4():
		if r.cfg.depth == 128 {
			bits -= 96
			if bits < 0 {
				// The IPv4 form cannot express this prefix. Use the raw form.
				addr = a.addr16()
				bits += 96
			}
		}
	case addr.Is4In6():
		if r.cfg.depth == 32 {
			bits += 96
		}
	}
	return netip.PrefixFrom(addr, bits)
}

func (r *modelRun) genRange() (netip.Addr, netip.Addr) {
	a := r.genModelAddr()
	var b u128
	if r.src.byte() < 192 {
		b = a
		for range r.src.intn(300) {
			if b == lowMask(r.cfg.depth) {
				break
			}
			b = b.add1()
		}
	} else {
		b = r.genModelAddr()
	}
	start := r.apiAddr(a)
	end := r.apiAddr(b)
	if r.src.byte() < 224 {
		// Mostly keep the families equal, as most callers do.
		if start.Is4() && !end.Is4() && b.hi == 0 && b.lo>>32 == 0 {
			end = b.addr4()
		}
		if !start.Is4() && end.Is4() && r.cfg.depth == 128 {
			end = b.addr16()
		}
	}
	return start, end
}

func (r *modelRun) genValue() mmdbtype.DataType {
	c := r.src.byte()
	switch {
	case c < 128:
		return r.values[int(c)%8]
	case c < 228:
		return r.values[8+int(c)%(len(r.values)-8-modelInvalidValues)]
	case c < 236:
		return genRandomValue(r.src, 0, false)
	case c < 248 && len(r.views) > 0:
		return r.views[int(c)%len(r.views)]
	}
	return r.values[len(r.values)-modelInvalidValues+int(c)%modelInvalidValues]
}

func (r *modelRun) genPure() namedPure {
	return modelPureFuncs[r.src.intn(len(modelPureFuncs))]
}

func (r *modelRun) step(i int) error {
	op := r.src.intn(32)
	switch {
	case op >= 25 && op < 27:
		return r.burst(i)
	case op < 8 || op >= 27:
		p, v := r.genPrefix(), r.genValue()
		return r.insertPrefix(i, "Insert", p, v, nil, false)
	case op < 12:
		p, v, f := r.genPrefix(), r.genValue(), r.genPure()
		return r.insertPrefix(i, "InsertPureFunc/"+f.name, p, v, &f, false)
	case op < 15:
		p, v, f := r.genPrefix(), r.genValue(), r.genPure()
		return r.insertPrefix(i, "InsertFunc/"+f.name, p, v, &f, true)
	case op < 17:
		s, e := r.genRange()
		return r.insertRange(i, "InsertRange", s, e, r.genValue(), nil, false)
	case op < 18:
		s, e := r.genRange()
		f := r.genPure()
		return r.insertRange(i, "InsertRangePureFunc/"+f.name, s, e, r.genValue(), &f, false)
	case op < 19:
		s, e := r.genRange()
		f := r.genPure()
		return r.insertRange(i, "InsertRangeFunc/"+f.name, s, e, r.genValue(), &f, true)
	case op < 22:
		a := r.genModelAddr()
		r.trace = append(r.trace, fmt.Sprintf("%d: Get probe %s", i, a.addr16()))
		return r.checkAddr(a, true)
	case op < 23:
		if len(r.views) == 0 {
			return nil
		}
		p := r.genPrefix()
		v := r.views[r.src.intn(len(r.views))]
		// A reslice shares the view's backing array but holds less.
		switch view := v.(type) {
		case mmdbtype.Slice:
			v = view[:r.src.intn(len(view)+1)]
		case mmdbtype.Bytes:
			v = view[:r.src.intn(len(view)+1)]
		}
		return r.insertPrefix(i, "Insert(view)", p, v, nil, false)
	case op < 24:
		r.trace = append(r.trace, fmt.Sprintf("%d: WriteTo", i))
		return r.fullCheck()
	default:
		if r.src.byte() < 96 {
			return r.naiveLoad(i)
		}
		return r.loadRoundTrip(i)
	}
}

// burst inserts a repeating pattern of adjacent small prefixes at one or two
// aligned bases. The tree then holds equal subtrees that WriteTo can share.
func (r *modelRun) burst(i int) error {
	bases := []u128{r.genModelAddr()}
	if r.src.byte() < 128 {
		bases = append(bases, r.genModelAddr())
	}
	host := r.src.intn(3)
	count := 2 + r.src.intn(15)
	pattern := make([]mmdbtype.DataType, 1+r.src.intn(3))
	for j := range pattern {
		pattern[j] = r.genValue()
	}
	bits := r.cfg.depth - host
	for _, base := range bases {
		start := prefixRange(base, bits-5, r.cfg.depth).first
		for j := range count {
			p := r.apiPrefix(start, bits)
			err := r.insertPrefix(i, "Insert(burst)", p, pattern[j%len(pattern)], nil, false)
			if err != nil {
				return err
			}
			for range 1 << host {
				if start == lowMask(r.cfg.depth) {
					return nil
				}
				start = start.add1()
			}
		}
	}
	return nil
}

// insertPrefix runs one prefix insert on the tree and the model. A nil fn
// selects Insert, which uses Options.Inserter.
func (r *modelRun) insertPrefix(
	i int,
	name string,
	p netip.Prefix,
	v mmdbtype.DataType,
	fn *namedPure,
	withMetadata bool,
) error {
	var calls []modelCall
	logged := r.v1 != nil && r.v1.logPrefix(r, p, v, fn)
	err, panicked := callTree(func() error {
		switch {
		case fn == nil:
			return r.tree.Insert(p, v)
		case withMetadata:
			return r.tree.InsertFunc(p, v, r.recordingFunc(fn.fn, &calls))
		default:
			return r.tree.InsertPureFunc(p, v, fn.fn)
		}
	})
	r.trace = append(r.trace, fmt.Sprintf(
		"%d: %s(%s, %s) -> %v%s", i, name, p, canon(v), err, panicNote(panicked),
	))
	if logged {
		r.v1.result(r.trace[len(r.trace)-1], err)
	}
	defer func() {
		switch {
		case r.v1 == nil:
		case logged:
			r.v1.done(r)
		default:
			r.v1.skipped(r)
		}
	}()

	var pieces []mrange
	want := modelErrNone
	rng, ok := r.model.insertRange(p)
	if ok {
		pieces = []mrange{rng}
		r.addProbes(rng)
	} else {
		want = modelErrInput
	}
	return r.finishInsert(want, pieces, v, fn, calls, withMetadata, false, panicked, err)
}

func (r *modelRun) insertRange(
	i int,
	name string,
	start, end netip.Addr,
	v mmdbtype.DataType,
	fn *namedPure,
	withMetadata bool,
) error {
	var calls []modelCall
	logged := r.v1 != nil && r.v1.logRange(r, start, end, v, fn)
	err, panicked := callTree(func() error {
		switch {
		case fn == nil:
			return r.tree.InsertRange(start, end, v)
		case withMetadata:
			return r.tree.InsertRangeFunc(start, end, v, r.recordingFunc(fn.fn, &calls))
		default:
			return r.tree.InsertRangePureFunc(start, end, v, fn.fn)
		}
	})
	r.trace = append(r.trace, fmt.Sprintf(
		"%d: %s(%s, %s, %s) -> %v%s", i, name, start, end, canon(v), err, panicNote(panicked),
	))
	if logged {
		r.v1.result(r.trace[len(r.trace)-1], err)
	}
	defer func() {
		switch {
		case r.v1 == nil:
		case logged:
			r.v1.done(r)
		default:
			r.v1.skipped(r)
		}
	}()

	want := modelErrNone
	var pieces []mrange
	s, e := start.Unmap(), end.Unmap()
	switch {
	case !s.IsValid() || !e.IsValid():
		want = modelErrInput
	case s.Is4() != e.Is4():
		want = modelErrInput
	case r.cfg.depth == 32 && !s.Is4():
		want = modelErrInput
	default:
		var rng mrange
		if s.Is4() {
			rng = mrange{u128FromAddr4(s), u128FromAddr4(e)}
		} else {
			rng = mrange{u128FromAddr16(s), u128FromAddr16(e)}
		}
		if rng.first.cmp(rng.last) > 0 {
			want = modelErrInput
			break
		}
		pieces = rangePrefixes(rng, r.cfg.depth)
		r.addProbes(rng)
	}
	return r.finishInsert(want, pieces, v, fn, calls, withMetadata, true, panicked, err)
}

// modelCall is one InsertFunc callback: its arguments and the lookups it
// made.
type modelCall struct {
	existing mmdbtype.DataType
	metadata inserter.Metadata
	gets     []callbackGet
}

// callbackGet is a Get that a callback made during an insert.
type callbackGet struct {
	query  netip.Addr
	prefix netip.Prefix
	value  mmdbtype.DataType
}

// recordingFunc wraps a pure function as an inserter.Func. Each call records
// its arguments and looks up addresses around the insert. The first call also
// tries a reentrant mutation, which must fail without effect.
func (r *modelRun) recordingFunc(fn inserter.PureFunc, calls *[]modelCall) inserter.Func {
	return func(
		old, newValue mmdbtype.DataType,
		metadata inserter.Metadata,
	) (mmdbtype.DataType, error) {
		if len(*calls) == 0 && r.callbackErr == nil {
			r.callbackErr = r.tryReentrantMutation(metadata.InsertedNetwork)
		}
		*calls = append(*calls, modelCall{
			existing: old,
			metadata: metadata,
			gets:     r.callbackGets(metadata),
		})
		return fn(old, newValue)
	}
}

// callbackGets looks up the ends of the existing record and of the inserted
// network, and the addresses next to the inserted network.
func (r *modelRun) callbackGets(metadata inserter.Metadata) []callbackGet {
	existing := r.model.prefixToRange(metadata.ExistingNetwork())
	inserted := r.model.prefixToRange(metadata.InsertedNetwork)
	addrs := []u128{existing.first, existing.last, inserted.first, inserted.last}
	if inserted.first != (u128{}) {
		addrs = append(addrs, inserted.first.sub1())
	}
	if inserted.last != lowMask(r.cfg.depth) {
		addrs = append(addrs, inserted.last.add1())
	}
	var gets []callbackGet
	for _, a := range addrs {
		if r.model.resolveAlias(a) != a {
			// Alias lookups can hit the known compressed-path bug.
			continue
		}
		query := a.addr16()
		if r.cfg.depth == 32 || (a.hi == 0 && a.lo>>32 == 0) {
			query = a.addr4()
		}
		prefix, value := r.tree.Get(query)
		gets = append(gets, callbackGet{query: query, prefix: prefix, value: value})
	}
	return gets
}

// tryReentrantMutation tries one mutation of the tree from inside a callback.
// It returns an error if the tree accepts the mutation.
func (r *modelRun) tryReentrantMutation(network netip.Prefix) error {
	value := mmdbtype.String("reentrant")
	var err error
	var written int
	op := len(r.trace) % 4
	switch op {
	case 0:
		err = r.tree.Insert(network, value)
	case 1:
		err = r.tree.InsertRange(network.Addr(), network.Addr(), value)
	case 2:
		err = r.tree.InsertPureFunc(network, value, inserter.Replace)
	case 3:
		var buf bytes.Buffer
		_, err = r.tree.WriteTo(&buf)
		written = buf.Len()
	}
	if !errors.Is(err, errReentrantMutation) || written != 0 {
		return r.fail("reentrant", "reentrant operation %d on %s returned %v and wrote %d bytes",
			op, network, err, written)
	}
	return nil
}

func (r *modelRun) finishInsert(
	want modelErrKind,
	pieces []mrange,
	v mmdbtype.DataType,
	fn *namedPure,
	calls []modelCall,
	withMetadata bool,
	isRange bool,
	panicked bool,
	got error,
) error {
	if err := r.callbackErr; err != nil {
		return err
	}
	if panicked {
		return r.afterPanic(pieces, v, fn)
	}
	if want == modelErrNone {
		var pure inserter.PureFunc
		switch {
		case fn != nil:
			pure = fn.modelFunc()
		case r.optsInserter != nil:
			pure = r.optsInserter.modelFunc()
		}
		var resolve modelResolve
		if pure == nil {
			// The direct-value path validates the value before it walks.
			if v != nil && !validValue(v) {
				want = modelErrValue
			}
			resolve = func(mmdbtype.DataType) (mmdbtype.DataType, error) { return v, nil }
		} else {
			resolve = func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
				return pure(old, v)
			}
		}
		if want == modelErrNone {
			var states []pieceState
			want = r.model.apply(pieces, resolve, &states)
			if withMetadata {
				if err := r.checkCalls(states, calls, want, isRange); err != nil {
					return err
				}
			}
		}
	}
	if err := r.checkErr(want, got); err != nil {
		return err
	}
	if err := r.tree.auditValueStore(); err != nil {
		return r.fail("audit", "%v", err)
	}
	return r.checkProbes()
}

func panicNote(panicked bool) string {
	if panicked {
		return " (panic)"
	}
	return ""
}

// afterPanic checks the tree after an inserter panicked. The insertion
// guarantees do not apply after a panic, so the check is weak: the tree
// passes the audit and accepts new operations, and every address holds its
// old value or its new value. The model then takes the tree's values, read
// back with the reader.
func (r *modelRun) afterPanic(pieces []mrange, v mmdbtype.DataType, fn *namedPure) error {
	if fn == nil {
		return r.fail("panic", "a direct insert panicked")
	}
	if r.tree.mutating {
		return r.fail("panic", "the tree still rejects mutations after a panic")
	}
	if err := r.tree.auditValueStore(); err != nil {
		return r.fail("audit", "after a panic: %v", err)
	}
	post := r.model.clone()
	model := fn.modelFunc()
	post.apply(pieces, func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
		return model(old, v)
	}, nil)
	var buf bytes.Buffer
	if _, err := r.tree.WriteTo(&buf); err != nil {
		return r.fail("write", "after a panic: %v", err)
	}
	reader, err := maxminddb.OpenBytes(buf.Bytes())
	if err != nil {
		return r.fail("reader-open", "after a panic: %v", err)
	}
	defer reader.Close()
	synced := newTreeModel(r.cfg)
	var u mmdbtype.Unmarshaler
	for res := range reader.Networks(maxminddb.IncludeAliasedNetworks()) {
		if err := res.Err(); err != nil {
			return r.fail("reader-networks", "after a panic: %v", err)
		}
		nr := r.model.prefixToRange(res.Prefix())
		if _, ok := r.model.resolveAliasRange(nr); !ok || r.model.resolveAlias(nr.first) != nr.first {
			continue // Aliased networks repeat the IPv4 subtree.
		}
		u.Clear()
		if err := res.Decode(&u); err != nil {
			return r.fail("reader-decode", "after a panic: %v", err)
		}
		value := normalizeValue(u.Result())
		key := canon(value)
		for _, run := range r.model.runs(nr) {
			for _, sub := range post.runs(run) {
				if key != r.model.get(sub.first).key && key != post.get(sub.first).key {
					return r.fail("panic-state", "after a panic %s holds %s, old %s, new %s",
						res.Prefix(), key, r.model.get(sub.first).key, post.get(sub.first).key)
				}
			}
		}
		synced.set(nr, value)
	}
	r.model = synced
	return r.checkProbes()
}

func (r *modelRun) checkErr(want modelErrKind, got error) error {
	var audit *RefcountAuditError
	if errors.As(got, &audit) {
		return r.fail("audit-error", "%v", got)
	}
	if (want == modelErrNone) != (got == nil) {
		return r.fail("error-agreement", "model expects %s, tree returned %v", want, got)
	}
	switch want {
	case modelErrAliased:
		var e *AliasedNetworkError
		if !errors.As(got, &e) {
			return r.fail("error-kind", "model expects an aliased network error, got %v", got)
		}
	case modelErrReserved:
		var e *ReservedNetworkError
		if !errors.As(got, &e) {
			return r.fail("error-kind", "model expects a reserved network error, got %v", got)
		}
	}
	return nil
}

func (r *modelRun) addProbes(rng mrange) {
	r.probes = append(r.probes, rng.first, rng.last)
	if rng.first != (u128{}) {
		r.probes = append(r.probes, rng.first.sub1())
	}
	if rng.last != lowMask(r.cfg.depth) {
		r.probes = append(r.probes, rng.last.add1())
	}
	if n := len(r.probes); n > 24 {
		r.probes = r.probes[n-24:]
	}
}

func (r *modelRun) checkProbes() error {
	for _, a := range r.probes {
		if err := r.checkAddr(a, false); err != nil {
			return err
		}
	}
	for _, a := range r.anchors {
		if err := r.checkAddr(a, false); err != nil {
			return err
		}
	}
	return nil
}

// checkAddr checks Get for every form of a tree-space address.
func (r *modelRun) checkAddr(a u128, keepView bool) error {
	forms := []netip.Addr{}
	if r.cfg.depth == 32 {
		forms = append(forms, a.addr4(), netip.AddrFrom16(a.addr4().As16()))
	} else {
		forms = append(forms, a.addr16())
		if a.hi == 0 && a.lo>>32 == 0 {
			forms = append(forms, a.addr4(), netip.AddrFrom16(a.addr4().As16()))
		}
	}
	for _, addr := range forms {
		if err := r.checkGet(addr, keepView); err != nil {
			return err
		}
	}
	return nil
}

func (r *modelRun) checkGet(addr netip.Addr, keepView bool) error {
	prefix, value := r.tree.Get(addr)
	ma, ok := r.model.getAddr(addr)
	if !ok {
		if prefix.IsValid() || value != nil {
			return r.fail("get-invalid", "Get(%s) = %s %s, want zero", addr, prefix, canon(value))
		}
		return nil
	}
	want := r.model.get(r.model.resolveAlias(ma))
	err := r.checkGetResult(addr, ma, prefix, value, want.key)
	if err != nil && r.skipKnown && r.model.resolveAlias(ma) != ma && treeHasLivePaths(r.tree) {
		// Known bug: Get through the 2001::/32 and 2002::/16 aliases compares
		// compressed-path bits at alias-relative depths.
		return nil
	}
	if err != nil {
		return err
	}
	if keepView && value != nil {
		r.views = append(r.views, value)
		if len(r.views) > 16 {
			r.views = r.views[1:]
		}
	}
	return nil
}

func (r *modelRun) checkGetResult(
	addr netip.Addr,
	ma u128,
	prefix netip.Prefix,
	value mmdbtype.DataType,
	wantKey string,
) error {
	if canon(value) != wantKey {
		return r.fail("get-value", "Get(%s) = %s, model %s", addr, canon(value), wantKey)
	}
	if hasPointerTypes(value) {
		return r.fail("get-view", "Get(%s) returned a pointer type: %T", addr, value)
	}
	return r.checkReturnedPrefix("Get", addr, ma, prefix, wantKey)
}

// treeHasLivePaths reports whether the tree holds a compressed path that
// WriteTo has not expanded yet.
func treeHasLivePaths(tree *Tree) bool {
	for _, path := range tree.paths {
		if path.record.recordType != recordTypeRetired {
			return true
		}
	}
	return false
}

// checkReturnedPrefix checks that a returned prefix contains the query and
// that the model holds wantKey for every address in it.
func (r *modelRun) checkReturnedPrefix(
	who string,
	addr netip.Addr,
	ma u128,
	prefix netip.Prefix,
	wantKey string,
) error {
	if !prefix.IsValid() {
		return r.fail("prefix-invalid", "%s(%s) returned an invalid prefix", who, addr)
	}
	pr := r.model.prefixToRange(prefix)
	if !pr.contains(ma) {
		return r.fail("prefix-contains", "%s(%s) returned %s, which does not contain it",
			who, addr, prefix)
	}
	resolved, ok := r.model.resolveAliasRange(pr)
	if !ok {
		return r.fail("prefix-alias", "%s(%s) returned %s, which straddles an alias",
			who, addr, prefix)
	}
	seg, ok := r.model.constant(resolved)
	if !ok {
		return r.fail("prefix-constant", "%s(%s) returned %s, but the model is not constant there",
			who, addr, prefix)
	}
	if seg.key != wantKey {
		return r.fail("prefix-value", "%s(%s) returned %s holding %s, model %s",
			who, addr, prefix, wantKey, seg.key)
	}
	return nil
}

// checkCalls checks InsertFunc metadata against the model. Each call's
// existing record must be constant in the model as it stood before that
// piece, hold the existing value, and the calls for a piece must tile the
// piece's insertable addresses in order.
func (r *modelRun) checkCalls(
	states []pieceState,
	calls []modelCall,
	result modelErrKind,
	isRange bool,
) error {
	ci := 0
	for si, st := range states {
		parts := st.model.subtractExcluded(st.piece)
		next, part := u128{}, 0
		if len(parts) > 0 {
			next = parts[0].first
		}
		done := len(parts) == 0
		for ; ci < len(calls); ci++ {
			call := calls[ci]
			// Range subnets are not normalized, so map the network raw.
			if r.model.prefixToRange(call.metadata.InsertedNetwork) != st.piece {
				break
			}
			// Known bug: range subnets are not normalized.
			if !(r.skipKnown && isRange) && call.metadata.InsertedNetwork.Addr().Is4In6() &&
				call.metadata.InsertedNetwork.Bits() >= 96 {
				return r.fail("metadata-normalization",
					"InsertedNetwork %s is IPv4-mapped", call.metadata.InsertedNetwork)
			}
			if call.metadata.TreeDepth != r.cfg.depth {
				return r.fail("metadata", "TreeDepth %d", call.metadata.TreeDepth)
			}
			if got, want := call.metadata.InsertedDepth(), rangeDepth(st.piece, r.cfg.depth); got != want {
				return r.fail("metadata", "InsertedDepth %d for %s, want %d",
					got, call.metadata.InsertedNetwork, want)
			}
			existing := call.metadata.ExistingNetwork()
			if !existing.IsValid() {
				return r.fail("metadata", "invalid ExistingNetwork for %+v", call.metadata)
			}
			er := r.model.prefixToRange(existing)
			if got, want := call.metadata.ExistingDepth, rangeDepth(er, r.cfg.depth); got != want {
				return r.fail("metadata", "ExistingDepth %d for %s", got, existing)
			}
			if got, want := call.metadata.ExistingAddr, treeLayout(er.first, r.cfg.depth); got != want {
				return r.fail("metadata", "ExistingAddr %x for %s, want %x", got, existing, want)
			}
			seg, ok := st.model.constant(er)
			if !ok || seg.key != canon(call.existing) {
				return r.fail("metadata-existing",
					"call %d: ExistingNetwork %s, existing %s, model constant=%v %s",
					ci, existing, canon(call.existing), ok, seg.key)
			}
			post := r.model
			if si+1 < len(states) {
				post = states[si+1].model
			}
			if err := r.checkCallbackGets(call, st, post, er); err != nil {
				return err
			}
			if done {
				return r.fail("metadata-tiling", "call %d after the piece was covered", ci)
			}
			clipped := er
			if clipped.first.cmp(st.piece.first) < 0 {
				clipped.first = st.piece.first
			}
			if clipped.last.cmp(st.piece.last) > 0 {
				clipped.last = st.piece.last
			}
			if clipped.first != next || !clipped.within(parts[part]) {
				return r.fail("metadata-tiling",
					"call %d: record %s does not continue at %s",
					ci, existing, next.addr16())
			}
			if clipped.last == parts[part].last {
				part++
				if part == len(parts) {
					done = true
				} else {
					next = parts[part].first
				}
			} else {
				next = clipped.last.add1()
			}
		}
		last := si == len(states)-1
		if !done && !(last && result != modelErrNone) {
			return r.fail("metadata-tiling", "piece %d not covered: next %s",
				si, next.addr16())
		}
	}
	if ci != len(calls) {
		return r.fail("metadata-tiling", "%d unexpected calls", len(calls)-ci)
	}
	return nil
}

// checkCallbackGets checks the lookups a callback made. Inside the piece,
// addresses before the current record hold their new values and the others
// hold their old values. Addresses outside the piece hold their old values.
func (r *modelRun) checkCallbackGets(
	call modelCall,
	st pieceState,
	post *treeModel,
	record mrange,
) error {
	for _, g := range call.gets {
		ma, ok := r.model.getAddr(g.query)
		if !ok {
			return r.fail("callback-get", "cannot map %s", g.query)
		}
		want := st.model.get(ma)
		if st.piece.contains(ma) && ma.cmp(record.first) < 0 {
			want = post.get(ma)
		}
		if canon(g.value) != want.key {
			return r.fail("callback-get", "Get(%s) in a callback for record %s = %s, model %s",
				g.query, call.metadata.ExistingNetwork(), canon(g.value), want.key)
		}
		if !g.prefix.IsValid() || !r.model.prefixToRange(g.prefix).contains(ma) {
			return r.fail("callback-get", "Get(%s) in a callback returned %s",
				g.query, g.prefix)
		}
	}
	return nil
}

// rangeDepth returns the prefix length, in tree bits, of a prefix range.
func rangeDepth(r mrange, depth int) int {
	diff := u128{r.first.hi ^ r.last.hi, r.first.lo ^ r.last.lo}
	host := bits.Len64(diff.lo)
	if diff.hi != 0 {
		host = 64 + bits.Len64(diff.hi)
	}
	return depth - host
}

// treeLayout returns a tree-space address in the byte layout that
// inserter.Metadata.ExistingAddr uses.
func treeLayout(a u128, depth int) [16]byte {
	var out [16]byte
	if depth == 32 {
		v := uint32(a.lo) //nolint:gosec // IPv4 tree addresses fit.
		out[0], out[1], out[2], out[3] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v)
		return out
	}
	return a.addr16().As16()
}

// fullCheck writes the tree, reads it with maxminddb-golang, and compares
// every network with the model.
func (r *modelRun) fullCheck() error {
	var buf bytes.Buffer
	n, err := r.tree.WriteTo(&buf)
	if err != nil {
		return r.fail("write", "%v", err)
	}
	if n != int64(buf.Len()) {
		return r.fail("write-count", "WriteTo returned %d for %d bytes", n, buf.Len())
	}
	if r.onWrite != nil {
		r.onWrite(r, buf.Bytes())
	}
	var again bytes.Buffer
	if _, err := r.tree.WriteTo(&again); err != nil {
		return r.fail("write", "second write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), again.Bytes()) {
		return r.fail("write-determinism", "two writes differ")
	}
	reader, err := maxminddb.OpenBytes(buf.Bytes())
	if err != nil {
		return r.fail("reader-open", "%v", err)
	}
	defer reader.Close()
	if err := reader.Verify(); err != nil && !(r.skipKnown && r.modelHasInvalidUTF8()) {
		return r.fail("reader-verify", "%v", err)
	}
	if err := r.checkFileMetadata(reader, buf.Bytes()); err != nil {
		return err
	}
	if err := r.checkNetworks(reader); err != nil {
		return err
	}
	if err := r.checkNetworksSkippingAliases(reader); err != nil {
		return err
	}
	for _, a := range append(append([]u128{}, r.probes...), r.anchors...) {
		if err := r.checkReaderAddr(reader, a); err != nil {
			return err
		}
	}
	return nil
}

// checkFileMetadata checks the metadata section and the data section
// separator of a written database.
func (r *modelRun) checkFileMetadata(reader *maxminddb.Reader, db []byte) error {
	md := reader.Metadata
	recordSize := r.opts.RecordSize
	if recordSize == 0 {
		// The test databases are small, so the smallest size always fits.
		recordSize = 24
	}
	switch {
	case md.BinaryFormatMajorVersion != 2 || md.BinaryFormatMinorVersion != 0:
		return r.fail("file-metadata", "format version %d.%d",
			md.BinaryFormatMajorVersion, md.BinaryFormatMinorVersion)
	case md.BuildEpoch != 1:
		return r.fail("file-metadata", "build epoch %d", md.BuildEpoch)
	case md.DatabaseType != r.opts.DatabaseType:
		return r.fail("file-metadata", "database type %q", md.DatabaseType)
	case len(md.Description) != 1 || md.Description["en"] != r.opts.Description["en"]:
		return r.fail("file-metadata", "description %v", md.Description)
	case int(md.IPVersion) != r.opts.IPVersion:
		return r.fail("file-metadata", "IP version %d", md.IPVersion)
	case len(md.Languages) != 0:
		return r.fail("file-metadata", "languages %v", md.Languages)
	case int(md.RecordSize) != recordSize:
		return r.fail("file-metadata", "record size %d, want %d", md.RecordSize, recordSize)
	}
	treeSize := int(md.NodeCount) * int(md.RecordSize) / 4
	if treeSize+16 > len(db) || !bytes.Equal(db[treeSize:treeSize+16], make([]byte, 16)) {
		return r.fail("file-metadata", "no data section separator after %d nodes", md.NodeCount)
	}
	return nil
}

func (r *modelRun) checkNetworks(reader *maxminddb.Reader) error {
	next := u128{}
	done := false
	end := lowMask(r.cfg.depth)
	var u mmdbtype.Unmarshaler
	for res := range reader.Networks(
		maxminddb.IncludeNetworksWithoutData(),
		maxminddb.IncludeAliasedNetworks(),
	) {
		if err := res.Err(); err != nil {
			return r.fail("reader-networks", "%v", err)
		}
		if done {
			return r.fail("reader-tiling", "network %s after the end", res.Prefix())
		}
		nr := r.model.prefixToRange(res.Prefix())
		if nr.first != next {
			return r.fail("reader-tiling", "network %s, want start %s", res.Prefix(), next.addr16())
		}
		if nr.last == end {
			done = true
		} else {
			next = nr.last.add1()
		}
		var value mmdbtype.DataType
		if res.Found() {
			u.Clear()
			if err := res.Decode(&u); err != nil {
				return r.fail("reader-decode", "%s: %v", res.Prefix(), err)
			}
			value = u.Result()
		}
		resolved, ok := r.model.resolveAliasRange(nr)
		if !ok {
			return r.fail("reader-alias", "network %s straddles an alias", res.Prefix())
		}
		seg, ok := r.model.constant(resolved)
		if !ok {
			return r.fail("reader-constant", "network %s holds %s, but the model is not constant there",
				res.Prefix(), canon(value))
		}
		if seg.key != canon(value) {
			return r.fail("reader-value", "network %s holds %s, model %s",
				res.Prefix(), canon(value), seg.key)
		}
	}
	if !done {
		return r.fail("reader-tiling", "networks end before the address space, next %s", next.addr16())
	}
	return nil
}

// checkNetworksSkippingAliases checks that the default iteration, which skips
// aliases, covers every address outside the aliases exactly once.
func (r *modelRun) checkNetworksSkippingAliases(reader *maxminddb.Reader) error {
	// Known reader bug: when the IPv4 start is a data record, the default
	// iteration also skips every network outside ::/96 that has the same
	// value. skipKey holds that value, and the check accepts those gaps.
	skipKey := ""
	if r.skipKnown && r.cfg.depth == 128 && !r.cfg.aliasing {
		res := reader.Lookup(netip.IPv4Unspecified())
		if res.Found() && (res.Prefix().Addr().Is6() || res.Prefix().Bits() == 0) {
			skipKey = r.model.get(u128{}).key
		}
	}
	expected := subtractRanges(mrange{u128{}, lowMask(r.cfg.depth)}, aliasRanges(r.model.aliases))
	part := 0
	next := expected[0].first
	// skipGap moves next past a gap that the reader bug explains.
	skipGap := func(end u128, endsPart bool) bool {
		if skipKey == "" {
			return false
		}
		gap := mrange{next, end}
		if endsPart {
			gap.last = expected[part].last
		}
		ipv4 := prefixRange(u128{}, 96, 128)
		for _, run := range r.model.runs(gap) {
			if run.overlaps(ipv4) || r.model.get(run.first).key != skipKey {
				return false
			}
		}
		return true
	}
	for res := range reader.Networks(maxminddb.IncludeNetworksWithoutData()) {
		if err := res.Err(); err != nil {
			return r.fail("reader-networks", "%v", err)
		}
		nr := r.model.prefixToRange(res.Prefix())
		for part < len(expected) && nr.first.cmp(next) > 0 {
			if nr.first.cmp(expected[part].last) > 0 {
				if !skipGap(u128{}, true) {
					break
				}
				part++
				if part < len(expected) {
					next = expected[part].first
				}
				continue
			}
			if !skipGap(nr.first.sub1(), false) {
				break
			}
			next = nr.first
		}
		if part == len(expected) || nr.first != next || !nr.within(expected[part]) {
			return r.fail("reader-tiling-default", "network %s, want start %s",
				res.Prefix(), next.addr16())
		}
		if nr.last == expected[part].last {
			part++
			if part < len(expected) {
				next = expected[part].first
			}
		} else {
			next = nr.last.add1()
		}
	}
	for part < len(expected) && skipGap(u128{}, true) {
		part++
		if part < len(expected) {
			next = expected[part].first
		}
	}
	if part != len(expected) {
		return r.fail("reader-tiling-default", "networks end early, next %s", next.addr16())
	}
	return nil
}

func aliasRanges(aliases []aliasRegion) []mrange {
	var out []mrange
	for _, a := range aliases {
		out = append(out, a.r)
	}
	return mergeRanges(out)
}

func (r *modelRun) checkReaderAddr(reader *maxminddb.Reader, a u128) error {
	var forms []netip.Addr
	if r.cfg.depth == 32 {
		forms = append(forms, a.addr4())
	} else {
		forms = append(forms, a.addr16())
		if a.hi == 0 && a.lo>>32 == 0 {
			forms = append(forms, a.addr4(), netip.AddrFrom16(a.addr4().As16()))
		}
	}
	var u mmdbtype.Unmarshaler
	for _, addr := range forms {
		res := reader.Lookup(addr)
		if err := res.Err(); err != nil {
			return r.fail("reader-lookup", "Lookup(%s): %v", addr, err)
		}
		var value mmdbtype.DataType
		if res.Found() {
			u.Clear()
			if err := res.Decode(&u); err != nil {
				return r.fail("reader-decode", "Lookup(%s): %v", addr, err)
			}
			value = u.Result()
		}
		ma, _ := r.model.readerAddr(addr)
		want := r.model.get(r.model.resolveAlias(ma))
		if canon(value) != want.key {
			return r.fail("reader-lookup-value", "Lookup(%s) = %s, model %s",
				addr, canon(value), want.key)
		}
		if err := r.checkReturnedPrefix("Lookup", addr, ma, res.Prefix(), want.key); err != nil {
			return err
		}
		if r.strictGetReader {
			_, got := r.tree.Get(addr)
			if canon(got) != canon(value) {
				return r.fail("get-reader-agreement", "Get(%s) = %s, reader %s",
					addr, canon(got), canon(value))
			}
		}
	}
	return nil
}

// fileHasMappedNetworks reports whether Load would read a data network that
// is IPv4-mapped at /96 or longer from the file.
func (r *modelRun) fileHasMappedNetworks(path string) (bool, error) {
	if r.cfg.depth != 128 || r.cfg.aliasing {
		return false, nil
	}
	reader, err := maxminddb.Open(path)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	for res := range reader.Networks(maxminddb.IncludeAliasedNetworks()) {
		if err := res.Err(); err != nil {
			return false, err
		}
		if p := res.Prefix(); p.Addr().Is4In6() && p.Bits() >= 96 {
			return true, nil
		}
	}
	return false, nil
}

// loadRoundTrip writes the tree, loads it, and continues with the loaded
// tree. Half of the loads use the tree's own options. The others change the
// aliasing, reserved-network, IP version, record size, or inserter options.
func (r *modelRun) loadRoundTrip(i int) error {
	if r.v1 != nil {
		r.v1.stop(r)
	}
	r.loads++
	path := filepath.Join(r.dir, fmt.Sprintf("load-%d.mmdb", r.loads))
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := r.tree.WriteTo(file); err != nil {
		file.Close()
		return r.fail("write", "%v", err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	target, targetInserter := r.genLoadOptions()
	effective := target
	if effective.IPVersion == 0 {
		effective.IPVersion = r.opts.IPVersion
	}
	cfg := configFor(effective)
	sameConfig := cfg == r.cfg
	if sameConfig && r.skipKnown {
		mapped, err := r.fileHasMappedNetworks(path)
		if err != nil {
			return r.fail("reader-networks", "%v", err)
		}
		if mapped {
			// Known bug: Load normalizes networks in ::ffff:0:0/96 to IPv4 and
			// inserts them over the IPv4 subtree.
			r.trace = append(r.trace, fmt.Sprintf("%d: Load round trip skipped", i))
			return nil
		}
	}
	loaded, loadErr := Load(path, target)
	name := "nil"
	if targetInserter != nil {
		name = targetInserter.name
	}
	r.trace = append(r.trace, fmt.Sprintf(
		"%d: Load(IPVersion:%d DisableIPv4Aliasing:%v IncludeReservedNetworks:%v RecordSize:%d Inserter:%s) -> %v",
		i, target.IPVersion, target.DisableIPv4Aliasing, target.IncludeReservedNetworks,
		target.RecordSize, name, loadErr,
	))

	var want *treeModel
	var wantErr bool
	if sameConfig {
		want, wantErr = r.roundTripModel(targetInserter)
	} else {
		want, wantErr, err = replayModel(path, cfg, targetInserter)
		if err != nil {
			return r.fail("reader-networks", "%v", err)
		}
	}
	return r.adoptLoad(loaded, loadErr, want, wantErr, effective, targetInserter)
}

// adoptLoad checks a load's error against the model. After a successful
// load, the run continues with the loaded tree, the expected model, and the
// load's options.
func (r *modelRun) adoptLoad(
	loaded *Tree,
	loadErr error,
	want *treeModel,
	wantErr bool,
	opts Options,
	named *namedPure,
) error {
	if wantErr != (loadErr != nil) {
		return r.fail("load-error", "model expects error=%v, Load returned %v", wantErr, loadErr)
	}
	if loadErr != nil {
		return nil
	}
	loaded.disableInsertCursor = r.tree.disableInsertCursor
	r.tree = loaded
	r.model = want
	r.opts = opts
	r.optsInserter = named
	if cfg := configFor(opts); cfg != r.cfg {
		r.cfg = cfg
		r.anchors = modelAnchors(cfg)
		r.probes = nil
	}
	if err := r.tree.auditValueStore(); err != nil {
		return r.fail("audit", "after load: %v", err)
	}
	if err := r.checkProbes(); err != nil {
		return err
	}
	return r.fullCheck()
}

// naiveLoad writes the model with the naive writer, checks the file with the
// reader, and loads it with the tree's options. The loaded tree must hold
// the model's values.
func (r *modelRun) naiveLoad(i int) error {
	if r.v1 != nil {
		r.v1.stop(r)
	}
	if r.skipKnown && r.modelHasMappedData() {
		// Known bug: Load normalizes networks in ::ffff:0:0/96 to IPv4 and
		// inserts them over the IPv4 subtree.
		r.trace = append(r.trace, fmt.Sprintf("%d: naive Load skipped", i))
		return nil
	}
	db, err := writeNaiveDatabase(r.src, r.model)
	if err != nil {
		return r.fail("naive-writer", "%v", err)
	}
	data := db.data
	reader, err := maxminddb.OpenBytes(data)
	if err != nil {
		return r.fail("naive-writer", "open: %v", err)
	}
	defer reader.Close()
	if !db.recordPointers && !db.duplicateKeys && !r.modelHasInvalidUTF8() {
		if err := reader.Verify(); err != nil {
			return r.fail("naive-writer", "verify: %v", err)
		}
	}
	if err := r.checkNetworks(reader); err != nil {
		return fmt.Errorf("[naive-writer] %w", err)
	}
	if err := r.checkNetworksSkippingAliases(reader); err != nil {
		return fmt.Errorf("[naive-writer] %w", err)
	}
	r.loads++
	path := filepath.Join(r.dir, fmt.Sprintf("naive-%d.mmdb", r.loads))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	loaded, loadErr := Load(path, r.opts)
	r.trace = append(r.trace, fmt.Sprintf("%d: naive Load (%d bytes) -> %v", i, len(data), loadErr))
	want, wantErr := r.roundTripModel(r.optsInserter)
	wantErr = wantErr || db.duplicateKeys
	return r.adoptLoad(loaded, loadErr, want, wantErr, r.opts, r.optsInserter)
}

// modelHasInvalidUTF8 reports whether the model holds a string or map key
// that is not valid UTF-8. The writer accepts such strings, and
// Reader.Verify rejects them.
func (r *modelRun) modelHasInvalidUTF8() bool {
	for _, seg := range r.model.segs {
		if hasInvalidUTF8(seg.value) {
			return true
		}
	}
	return false
}

// modelHasMappedData reports whether the raw ::ffff:0:0/96 region holds data
// in a tree without aliasing.
func (r *modelRun) modelHasMappedData() bool {
	if r.cfg.depth != 128 || r.cfg.aliasing {
		return false
	}
	mapped := r.model.prefixToRange(netip.MustParsePrefix("::ffff:0:0/96"))
	for _, run := range r.model.runs(mapped) {
		if r.model.get(run.first).value != nil {
			return true
		}
	}
	return false
}

// genLoadOptions returns the options for a load. Half of the time they are
// the tree's own options.
func (r *modelRun) genLoadOptions() (Options, *namedPure) {
	opts := r.opts
	named := r.optsInserter
	b := r.src.byte()
	if b < 128 {
		return opts, named
	}
	if b&1 != 0 {
		opts.DisableIPv4Aliasing = !opts.DisableIPv4Aliasing
	}
	if b&2 != 0 {
		opts.IncludeReservedNetworks = !opts.IncludeReservedNetworks
	}
	switch (b >> 2) & 3 {
	case 1:
		opts.IPVersion = 0 // Use the version in the file's metadata.
	case 2:
		opts.IPVersion = 10 - opts.IPVersion // Switch between 4 and 6.
	}
	opts.RecordSize = []int{0, 24, 28, 32}[(b>>4)&3]
	named = nil
	opts.Inserter = nil
	if choice := r.src.intn(4); choice != 0 {
		f := modelPureFuncs[[]int{0, 0, 3, 6}[choice]] // Replace, DeepMerge, wrapOld
		named = &f
		opts.Inserter = f.fn
	}
	return opts, named
}

// roundTripModel returns the model after a load with the tree's own
// configuration: each value v becomes inserter(nil, v), or stays v with no
// inserter.
func (r *modelRun) roundTripModel(named *namedPure) (*treeModel, bool) {
	want := newTreeModel(r.cfg)
	for si, seg := range r.model.segs {
		if seg.value == nil {
			continue
		}
		last := lowMask(r.cfg.depth)
		if si+1 < len(r.model.segs) {
			last = r.model.segs[si+1].start.sub1()
		}
		value := seg.value
		if named != nil {
			var err error
			value, err = named.modelFunc()(nil, normalizeValue(seg.value))
			if err != nil || (value != nil && !validValue(value)) {
				return nil, true
			}
		}
		want.set(mrange{seg.start, last}, normalizeValue(value))
	}
	return want, false
}

// replayModel returns the model after a load with a different configuration.
// It inserts each network that Load reads from the file into an empty model,
// in the order the reader yields them, with the documented insert rules.
func replayModel(path string, cfg modelConfig, named *namedPure) (*treeModel, bool, error) {
	reader, err := maxminddb.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer reader.Close()
	var iterOpts []maxminddb.NetworksOption
	if cfg.depth == 128 && !cfg.aliasing {
		iterOpts = append(iterOpts, maxminddb.IncludeAliasedNetworks())
	}
	m := newTreeModel(cfg)
	var u mmdbtype.Unmarshaler
	for res := range reader.Networks(iterOpts...) {
		if err := res.Err(); err != nil {
			return nil, false, err
		}
		u.Clear()
		if err := res.Decode(&u); err != nil {
			return nil, false, err
		}
		value := normalizeValue(u.Result())
		piece, ok := m.insertRange(res.Prefix())
		if !ok {
			return nil, true, nil
		}
		resolve := func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
			if named == nil {
				return value, nil
			}
			return named.modelFunc()(old, value)
		}
		if m.apply([]mrange{piece}, resolve, nil) != modelErrNone {
			return nil, true, nil
		}
	}
	return m, false, nil
}
