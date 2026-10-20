package mmdbwriter

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// This file holds a deliberately naive model of the Tree's observable
// behavior. It knows nothing about nodes, records, compressed paths, the
// value store, or hash-consing. It maps each address in tree space to a
// value, and it applies the documented insert rules one address range at a
// time.
//
// Tree space is the address space that the search tree indexes. For an IPv4
// tree it is 32 bits wide. For an IPv6 tree it is 128 bits wide, and the IPv4
// address a.b.c.d is ::a.b.c.d. In both cases the IPv4 address has the same
// numeric value, so u128{0, uint64(v4)} is that address in either tree.

type u128 struct{ hi, lo uint64 }

func (a u128) cmp(b u128) int {
	switch {
	case a.hi < b.hi:
		return -1
	case a.hi > b.hi:
		return 1
	case a.lo < b.lo:
		return -1
	case a.lo > b.lo:
		return 1
	}
	return 0
}

func (a u128) add1() u128 {
	lo := a.lo + 1
	hi := a.hi
	if lo == 0 {
		hi++
	}
	return u128{hi, lo}
}

func (a u128) sub1() u128 {
	lo := a.lo - 1
	hi := a.hi
	if a.lo == 0 {
		hi--
	}
	return u128{hi, lo}
}

func (a u128) and(b u128) u128 { return u128{a.hi & b.hi, a.lo & b.lo} }
func (a u128) or(b u128) u128  { return u128{a.hi | b.hi, a.lo | b.lo} }
func (a u128) not() u128       { return u128{^a.hi, ^a.lo} }

func (a u128) shr(n int) u128 {
	switch {
	case n == 0:
		return a
	case n >= 128:
		return u128{}
	case n >= 64:
		return u128{0, a.hi >> (n - 64)}
	}
	return u128{a.hi >> n, a.lo>>n | a.hi<<(64-n)}
}

// lowMask returns a value with the low n bits set.
func lowMask(n int) u128 {
	switch {
	case n <= 0:
		return u128{}
	case n < 64:
		return u128{0, 1<<n - 1}
	case n == 64:
		return u128{0, math.MaxUint64}
	case n < 128:
		return u128{1<<(n-64) - 1, math.MaxUint64}
	}
	return u128{math.MaxUint64, math.MaxUint64}
}

func u128FromAddr16(a netip.Addr) u128 {
	b := a.As16()
	var v u128
	for i := range 8 {
		v.hi = v.hi<<8 | uint64(b[i])
		v.lo = v.lo<<8 | uint64(b[i+8])
	}
	return v
}

func u128FromAddr4(a netip.Addr) u128 {
	b := a.As4()
	return u128{0, uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])}
}

func (a u128) addr16() netip.Addr {
	var b [16]byte
	for i := range 8 {
		b[7-i] = byte(a.hi >> (8 * i))
		b[15-i] = byte(a.lo >> (8 * i))
	}
	return netip.AddrFrom16(b)
}

func (a u128) addr4() netip.Addr {
	v := uint32(a.lo) //nolint:gosec // tree-space IPv4 addresses fit
	return netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
}

// mrange is an inclusive range of tree-space addresses.
type mrange struct{ first, last u128 }

func (r mrange) contains(a u128) bool {
	return r.first.cmp(a) <= 0 && a.cmp(r.last) <= 0
}

func (r mrange) within(outer mrange) bool {
	return outer.first.cmp(r.first) <= 0 && r.last.cmp(outer.last) <= 0
}

func (r mrange) overlaps(other mrange) bool {
	return r.first.cmp(other.last) <= 0 && other.first.cmp(r.last) <= 0
}

// prefixRange returns the range of a prefix that starts at start and is bits
// long in a depth-bit space. It masks start.
func prefixRange(start u128, bits, depth int) mrange {
	host := lowMask(depth - bits)
	first := start.and(host.not())
	return mrange{first, first.or(host)}
}

// rangePrefixes splits an inclusive range into the fewest aligned prefixes,
// in address order. It is written here, not borrowed from netipx, so the model
// shares no decomposition code with the tree.
func rangePrefixes(r mrange, depth int) []mrange {
	var out []mrange
	start := r.first
	for {
		// Grow the block while it stays aligned at start and inside r.
		host := 0
		for host < depth {
			next := lowMask(host + 1)
			if start.and(next) != (u128{}) {
				break
			}
			if start.or(next).cmp(r.last) > 0 {
				break
			}
			host++
		}
		block := mrange{start, start.or(lowMask(host))}
		out = append(out, block)
		if block.last == r.last {
			return out
		}
		start = block.last.add1()
	}
}

type modelConfig struct {
	depth    int  // 32 or 128
	aliasing bool // IPv4 aliases exist (IPv6 trees only)
	reserved bool // reserved networks are excluded
}

type aliasRegion struct {
	r     mrange
	depth int
}

type segment struct {
	start u128
	value mmdbtype.DataType
	key   string
}

type treeModel struct {
	cfg     modelConfig
	aliases []aliasRegion
	// excluded is sorted and disjoint. It holds the aliases and the reserved
	// networks. Inserts skip these addresses and never change their values.
	excluded []mrange
	// reservedOnly holds only the reserved networks, sorted and disjoint.
	reservedOnly []mrange
	// segs covers the whole space. segs[i] covers [segs[i].start,
	// segs[i+1].start-1]. Adjacent segments always differ.
	segs []segment
}

func newTreeModel(cfg modelConfig) *treeModel {
	m := &treeModel{
		cfg:  cfg,
		segs: []segment{{key: canon(nil)}},
	}
	if cfg.depth == 128 && cfg.aliasing {
		for _, a := range []string{"::ffff:0:0/96", "2001::/32", "2002::/16"} {
			p := netip.MustParsePrefix(a)
			m.aliases = append(m.aliases, aliasRegion{
				r:     prefixRange(u128FromAddr16(p.Addr()), p.Bits(), 128),
				depth: p.Bits(),
			})
		}
	}
	if cfg.reserved {
		// The model uses the package's reserved lists as data. The rules for
		// applying them are the model's own.
		for _, s := range reservedNetworksIPv4 {
			p := netip.MustParsePrefix(s)
			m.reservedOnly = append(m.reservedOnly, m.prefixToRange(p))
		}
		if cfg.depth == 128 {
			for _, s := range reservedNetworksIPv6 {
				p := netip.MustParsePrefix(s)
				m.reservedOnly = append(m.reservedOnly, m.prefixToRange(p))
			}
		}
	}
	m.reservedOnly = mergeRanges(m.reservedOnly)
	all := slices.Clone(m.reservedOnly)
	for _, a := range m.aliases {
		all = append(all, a.r)
	}
	m.excluded = mergeRanges(all)
	return m
}

func mergeRanges(rs []mrange) []mrange {
	slices.SortFunc(rs, func(a, b mrange) int { return a.first.cmp(b.first) })
	var out []mrange
	for _, r := range rs {
		if n := len(out); n > 0 &&
			(out[n-1].last == lowMask(128) || out[n-1].last.add1().cmp(r.first) >= 0) {
			if r.last.cmp(out[n-1].last) > 0 {
				out[n-1].last = r.last
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// prefixToRange maps a raw prefix into tree space. An IPv4 prefix maps into
// the IPv4 subtree. An IPv6 prefix, including an IPv4-mapped one, maps to its
// raw position. It does not apply the insert normalization rules.
func (m *treeModel) prefixToRange(p netip.Prefix) mrange {
	if p.Addr().Is4() {
		offset := 0
		if m.cfg.depth == 128 {
			offset = 96
		}
		return prefixRange(u128FromAddr4(p.Addr()), p.Bits()+offset, m.cfg.depth)
	}
	return prefixRange(u128FromAddr16(p.Addr()), p.Bits(), 128)
}

// insertRange applies the insert normalization rules that the Tree documents
// and the changelog lists. It returns false for a prefix that an insert must
// reject before it changes anything.
func (m *treeModel) insertRange(p netip.Prefix) (mrange, bool) {
	if !p.IsValid() {
		return mrange{}, false
	}
	addr := p.Addr()
	switch {
	case addr.Is4():
		return m.prefixToRange(p), true
	case addr.Is4In6():
		if p.Bits() < 96 {
			return mrange{}, false
		}
		return m.prefixToRange(netip.PrefixFrom(addr.Unmap(), p.Bits()-96)), true
	case m.cfg.depth == 32:
		return mrange{}, false
	}
	return m.prefixToRange(p), true
}

// getAddr maps an address the way Tree.Get does: it unmaps IPv4-mapped
// addresses first. It returns false for an address that Get cannot look up.
func (m *treeModel) getAddr(a netip.Addr) (u128, bool) {
	if !a.IsValid() {
		return u128{}, false
	}
	a = a.Unmap()
	if a.Is4() {
		return u128FromAddr4(a), true
	}
	if m.cfg.depth == 32 {
		return u128{}, false
	}
	return u128FromAddr16(a), true
}

// readerAddr maps an address the way a reader's Lookup does: an IPv4 address
// starts at the IPv4 subtree, and every IPv6 address, including an
// IPv4-mapped one, walks from the root.
func (m *treeModel) readerAddr(a netip.Addr) (u128, bool) {
	if !a.IsValid() {
		return u128{}, false
	}
	if a.Is4() {
		return u128FromAddr4(a), true
	}
	if m.cfg.depth == 32 {
		return u128{}, false
	}
	return u128FromAddr16(a), true
}

// resolveAlias maps an address inside an alias to the IPv4 address that it
// aliases. Other addresses are returned unchanged.
func (m *treeModel) resolveAlias(a u128) u128 {
	for _, alias := range m.aliases {
		if alias.r.contains(a) {
			return u128{0, a.shr(96-alias.depth).lo & math.MaxUint32}
		}
	}
	return a
}

// resolveAliasRange maps a range that lies inside one alias to the IPv4 range
// it aliases. A range that is not inside an alias is returned unchanged. The
// second result is false for a range that overlaps an alias without being
// inside it.
func (m *treeModel) resolveAliasRange(r mrange) (mrange, bool) {
	for _, alias := range m.aliases {
		if r.within(alias.r) {
			return mrange{m.resolveAlias(r.first), m.resolveAlias(r.last)}, true
		}
		if r.overlaps(alias.r) {
			return r, false
		}
	}
	return r, true
}

func (m *treeModel) segIndex(a u128) int {
	return sort.Search(len(m.segs), func(i int) bool {
		return m.segs[i].start.cmp(a) > 0
	}) - 1
}

func (m *treeModel) get(a u128) segment {
	return m.segs[m.segIndex(a)]
}

// constant reports whether every address in r has the same value, and
// returns that value.
func (m *treeModel) constant(r mrange) (segment, bool) {
	i := m.segIndex(r.first)
	if i+1 < len(m.segs) && m.segs[i+1].start.cmp(r.last) <= 0 {
		return segment{}, false
	}
	return m.segs[i], true
}

// runs returns the constant-value runs that cover r, in address order.
func (m *treeModel) runs(r mrange) []mrange {
	var out []mrange
	for i := m.segIndex(r.first); i < len(m.segs); i++ {
		first := m.segs[i].start
		if first.cmp(r.first) < 0 {
			first = r.first
		}
		if first.cmp(r.last) > 0 {
			break
		}
		last := r.last
		if i+1 < len(m.segs) && m.segs[i+1].start.sub1().cmp(last) < 0 {
			last = m.segs[i+1].start.sub1()
		}
		out = append(out, mrange{first, last})
		if last == r.last {
			break
		}
	}
	return out
}

func (m *treeModel) set(r mrange, value mmdbtype.DataType) {
	key := canon(value)
	m.split(r.first)
	if r.last != lowMask(m.cfg.depth) {
		m.split(r.last.add1())
	}
	i := m.segIndex(r.first)
	j := len(m.segs)
	if r.last != lowMask(m.cfg.depth) {
		j = m.segIndex(r.last.add1())
	}
	m.segs = slices.Replace(m.segs, i, j, segment{start: r.first, value: value, key: key})
	m.coalesce()
}

func (m *treeModel) split(a u128) {
	i := m.segIndex(a)
	if m.segs[i].start == a {
		return
	}
	s := m.segs[i]
	s.start = a
	m.segs = slices.Insert(m.segs, i+1, s)
}

func (m *treeModel) coalesce() {
	out := m.segs[:1]
	for _, s := range m.segs[1:] {
		if s.key == out[len(out)-1].key {
			continue
		}
		out = append(out, s)
	}
	m.segs = out
}

func (m *treeModel) clone() *treeModel {
	c := *m
	c.segs = slices.Clone(m.segs)
	return &c
}

// subtractExcluded returns the parts of r that inserts may change, in order.
func (m *treeModel) subtractExcluded(r mrange) []mrange {
	return subtractRanges(r, m.excluded)
}

// subtractRanges returns the parts of r outside the sorted ranges rs.
func subtractRanges(r mrange, rs []mrange) []mrange {
	out := []mrange{r}
	for _, e := range rs {
		var next []mrange
		for _, piece := range out {
			if !piece.overlaps(e) {
				next = append(next, piece)
				continue
			}
			if piece.first.cmp(e.first) < 0 {
				next = append(next, mrange{piece.first, e.first.sub1()})
			}
			if piece.last.cmp(e.last) > 0 {
				next = append(next, mrange{e.last.add1(), piece.last})
			}
		}
		out = next
	}
	return out
}

type modelErrKind int

const (
	modelErrNone modelErrKind = iota
	modelErrInput
	modelErrValue
	modelErrAliased
	modelErrReserved
	modelErrCallback
)

func (k modelErrKind) String() string {
	return [...]string{"none", "input", "value", "aliased", "reserved", "callback"}[k]
}

// modelResolve returns the value an address holds after an insert, given the
// value it held before. It returns an error to stop the insert.
type modelResolve func(old mmdbtype.DataType) (mmdbtype.DataType, error)

// pieceState records the model before one piece of an insert, so callback
// metadata can be checked against it.
type pieceState struct {
	piece mrange
	model *treeModel
}

// apply inserts each piece in order. A piece inside an alias or inside the
// reserved networks fails before it changes anything. Otherwise every run of
// equal values in the piece, outside the excluded networks, is resolved in
// address order, and the first failure stops the insert. Earlier runs keep
// their new values.
func (m *treeModel) apply(
	pieces []mrange,
	resolve modelResolve,
	states *[]pieceState,
) modelErrKind {
	for _, piece := range pieces {
		if states != nil {
			*states = append(*states, pieceState{piece: piece, model: m.clone()})
		}
		for _, alias := range m.aliases {
			if piece.within(alias.r) {
				return modelErrAliased
			}
		}
		// Reserved records merge only with each other, so a piece inside the
		// union of reserved networks fails. A piece that only aliases and
		// reserved networks cover, without being inside one alias, is
		// skipped without an error.
		if len(subtractRanges(piece, m.reservedOnly)) == 0 {
			return modelErrReserved
		}
		parts := m.subtractExcluded(piece)
		for _, part := range parts {
			for _, run := range m.runs(part) {
				old := m.get(run.first).value
				result, err := resolve(old)
				if err != nil {
					return modelErrCallback
				}
				if result != nil && !validValue(result) {
					return modelErrValue
				}
				m.set(run, normalizeValue(result))
			}
		}
	}
	return modelErrNone
}

// canon returns an encoding of a value that is independent of mmdbtype's
// Equal and of the value store. Floats are compared by bit pattern, pointer
// types are dereferenced, and map keys are sorted.
func canon(v mmdbtype.DataType) string {
	var b strings.Builder
	writeCanon(&b, v)
	return b.String()
}

func writeCanon(b *strings.Builder, v mmdbtype.DataType) {
	if d, ok := derefValue(v); ok {
		v = d
	} else {
		b.WriteString("nilptr")
		return
	}
	switch v := v.(type) {
	case nil:
		b.WriteString("nil")
	case mmdbtype.Bool:
		fmt.Fprintf(b, "bool(%t)", bool(v))
	case mmdbtype.Bytes:
		fmt.Fprintf(b, "bytes(%x)", []byte(v))
	case mmdbtype.Float32:
		fmt.Fprintf(b, "f32(%08x)", math.Float32bits(float32(v)))
	case mmdbtype.Float64:
		fmt.Fprintf(b, "f64(%016x)", math.Float64bits(float64(v)))
	case mmdbtype.Int32:
		fmt.Fprintf(b, "i32(%d)", int32(v))
	case mmdbtype.String:
		if len(v) > 40 {
			fmt.Fprintf(b, "str(len=%d,%x)", len(v), hashString(string(v)))
		} else {
			fmt.Fprintf(b, "str(%q)", string(v))
		}
	case mmdbtype.Uint16:
		fmt.Fprintf(b, "u16(%d)", uint16(v))
	case mmdbtype.Uint32:
		fmt.Fprintf(b, "u32(%d)", uint32(v))
	case mmdbtype.Uint64:
		fmt.Fprintf(b, "u64(%d)", uint64(v))
	case mmdbtype.Uint128:
		fmt.Fprintf(b, "u128(%x,%x)", v.High, v.Low)
	case mmdbtype.Pointer:
		fmt.Fprintf(b, "pointer(%d)", uint32(v))
	case mmdbtype.Map:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, string(k))
		}
		slices.Sort(keys)
		b.WriteString("map{")
		for _, k := range keys {
			fmt.Fprintf(b, "%q:", k)
			writeCanon(b, v[mmdbtype.String(k)])
			b.WriteString(",")
		}
		b.WriteString("}")
	case mmdbtype.Slice:
		b.WriteString("slice[")
		for _, e := range v {
			writeCanon(b, e)
			b.WriteString(",")
		}
		b.WriteString("]")
	default:
		fmt.Fprintf(b, "unknown(%T)", v)
	}
}

func hashString(s string) uint64 {
	// FNV-1a. Long strings only need a stable, readable fingerprint.
	h := uint64(14695981039346656037)
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// derefValue dereferences the pointer types whose DataType methods have value
// receivers. It returns false for a nil pointer.
func derefValue(v mmdbtype.DataType) (mmdbtype.DataType, bool) {
	switch p := v.(type) {
	case *mmdbtype.Bool:
		return derefPtr(p)
	case *mmdbtype.Bytes:
		return derefPtr(p)
	case *mmdbtype.Float32:
		return derefPtr(p)
	case *mmdbtype.Float64:
		return derefPtr(p)
	case *mmdbtype.Int32:
		return derefPtr(p)
	case *mmdbtype.Map:
		return derefPtr(p)
	case *mmdbtype.Pointer:
		return derefPtr(p)
	case *mmdbtype.Slice:
		return derefPtr(p)
	case *mmdbtype.String:
		return derefPtr(p)
	case *mmdbtype.Uint16:
		return derefPtr(p)
	case *mmdbtype.Uint32:
		return derefPtr(p)
	case *mmdbtype.Uint64:
		return derefPtr(p)
	case *mmdbtype.Uint128:
		return derefPtr(p)
	}
	return v, true
}

func derefPtr[T mmdbtype.DataType](p *T) (mmdbtype.DataType, bool) {
	if p == nil {
		return nil, false
	}
	return *p, true
}

// validValue reports whether the tree can store a non-nil value: no nil
// pointers, no nil container elements, and no raw Pointer values.
func validValue(v mmdbtype.DataType) bool {
	d, ok := derefValue(v)
	if !ok {
		return false
	}
	switch d := d.(type) {
	case nil:
		return false
	case mmdbtype.Pointer:
		return false
	case mmdbtype.Map:
		for _, c := range d {
			if !validValue(c) {
				return false
			}
		}
	case mmdbtype.Slice:
		for _, c := range d {
			if !validValue(c) {
				return false
			}
		}
	}
	return true
}

// normalizeValue returns a copy of a valid value in the form the tree
// returns: pointer types are dereferenced, and containers are fresh objects.
// The model passes this form to inserters, as the tree passes its
// materialized views.
func normalizeValue(v mmdbtype.DataType) mmdbtype.DataType {
	if v == nil {
		return nil
	}
	d, _ := derefValue(v)
	switch d := d.(type) {
	case mmdbtype.Map:
		out := make(mmdbtype.Map, len(d))
		for k, c := range d {
			out[k] = normalizeValue(c)
		}
		return out
	case mmdbtype.Slice:
		out := make(mmdbtype.Slice, len(d))
		for i, c := range d {
			out[i] = normalizeValue(c)
		}
		return out
	case mmdbtype.Bytes:
		return slices.Clone(d)
	}
	return d
}

// hasPointerTypes reports whether a value contains a pointer type. Views that
// the tree returns must not.
func hasPointerTypes(v mmdbtype.DataType) bool {
	if v == nil {
		return false
	}
	d, ok := derefValue(v)
	if !ok || reflect.TypeOf(d) != reflect.TypeOf(v) {
		return true
	}
	switch d := d.(type) {
	case mmdbtype.Map:
		for _, c := range d {
			if hasPointerTypes(c) {
				return true
			}
		}
	case mmdbtype.Slice:
		for _, c := range d {
			if hasPointerTypes(c) {
				return true
			}
		}
	}
	return false
}

var errModelCallback = errors.New("model test callback failure")
