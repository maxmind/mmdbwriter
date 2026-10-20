package mmdbwriter

import (
	"bytes"
	"fmt"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// runValueModel interprets data as a random value and a second value that
// differs from it in one place. It checks Copy, Equal, the value store, and
// the write, read, and Load round trips against canon, which shares no code
// with any of them.
func runValueModel(data []byte, dir string) (err error) {
	src := &byteSource{data: data}
	var v, other mmdbtype.DataType
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("[panic] %v\nvalue: %s\nother: %s", p, canon(v), canon(other))
		}
	}()
	v = genRandomValue(src, 0, true)
	other = mutateValue(src, v)
	fail := func(tag, format string, args ...any) error {
		return fmt.Errorf("[%s] %s\nvalue: %s\nother: %s",
			tag, fmt.Sprintf(format, args...), canon(v), canon(other))
	}
	if canon(other) == canon(v) {
		return fail("value-model", "the mutation did not change the value")
	}

	// Copy is equal and deep.
	before := canon(v)
	c := v.Copy()
	if canon(c) != before {
		return fail("copy", "copy is %s", canon(c))
	}
	scribble(c)
	if canon(v) != before {
		return fail("copy-deep", "changing the copy changed the value to %s", canon(v))
	}

	// Equal agrees with canon, in both directions.
	for _, pair := range [][2]mmdbtype.DataType{
		{v, v.Copy()}, {v, normalizeValue(v)}, {v, other}, {other, v},
	} {
		want := canon(pair[0]) == canon(pair[1])
		if got := pair[0].Equal(pair[1]); got != want {
			return fail("equal", "%s.Equal(%s) = %v", canon(pair[0]), canon(pair[1]), got)
		}
	}

	// The store interns by content, even when every value shares one hash.
	for _, store := range []*valueStore{
		newValueStore(),
		newValueStoreWithHash(func([]byte) uint64 { return 1 }),
	} {
		if err := checkStoreRoundTrip(store, v, other); err != nil {
			return fail("store", "%v", err)
		}
	}

	if err := checkWireRoundTrip(v, other, dir); err != nil {
		return fail("wire", "%v", err)
	}
	return nil
}

func checkStoreRoundTrip(store *valueStore, v, other mmdbtype.DataType) error {
	first, err := store.intern(v)
	if err != nil {
		return err
	}
	second, err := store.intern(v.Copy())
	if err != nil {
		return err
	}
	view := store.materialize(first)
	// A materialized view hits the identity cache.
	third, err := store.intern(view)
	if err != nil {
		return err
	}
	different, err := store.intern(other)
	if err != nil {
		return err
	}
	switch {
	case second != first || third != first:
		return fmt.Errorf("equal values interned as %d, %d, and %d", first, second, third)
	case different == first:
		return fmt.Errorf("different values interned as %d", first)
	case canon(view) != canon(v):
		return fmt.Errorf("materialized %s", canon(view))
	case hasPointerTypes(view):
		return fmt.Errorf("materialized view has a pointer type: %T", view)
	case canon(store.materialize(different)) != canon(other):
		return fmt.Errorf("materialized other as %s", canon(store.materialize(different)))
	}
	for _, ref := range []valueRef{first, second, third, different} {
		store.release(ref)
	}
	if live := liveValueNodeCount(store); live != 0 {
		return fmt.Errorf("%d nodes live after every reference was released", live)
	}
	return nil
}

// checkWireRoundTrip writes v and other into a tree, then checks them with
// the reader and after Load. other shares structure with v, so the writer
// uses pointers between them.
func checkWireRoundTrip(v, other mmdbtype.DataType, dir string) error {
	opts := Options{
		BuildEpoch:              1,
		DatabaseType:            "value-model-test",
		Description:             map[string]string{"en": "Value model test"},
		IPVersion:               4,
		IncludeReservedNetworks: true,
	}
	tree, err := New(opts)
	if err != nil {
		return err
	}
	if err := tree.Insert(netip.MustParsePrefix("0.0.0.0/1"), v); err != nil {
		return err
	}
	if err := tree.Insert(netip.MustParsePrefix("128.0.0.0/1"), other); err != nil {
		return err
	}
	var buf bytes.Buffer
	if _, err := tree.WriteTo(&buf); err != nil {
		return err
	}
	reader, err := maxminddb.OpenBytes(buf.Bytes())
	if err != nil {
		return err
	}
	defer reader.Close()
	// Known issue: the writer accepts strings that are not UTF-8, and
	// Verify rejects them.
	if !hasInvalidUTF8(v) && !hasInvalidUTF8(other) {
		if err := reader.Verify(); err != nil {
			return err
		}
	}
	path := filepath.Join(dir, "value-model.mmdb")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		return err
	}
	loaded, err := Load(path, opts)
	if err != nil {
		return err
	}
	for addr, want := range map[string]mmdbtype.DataType{"1.0.0.0": v, "200.0.0.0": other} {
		res := reader.Lookup(netip.MustParseAddr(addr))
		var u mmdbtype.Unmarshaler
		if err := res.Decode(&u); err != nil {
			return err
		}
		if canon(u.Result()) != canon(want) {
			return fmt.Errorf("reader decoded %s at %s", canon(u.Result()), addr)
		}
		_, got := loaded.Get(netip.MustParseAddr(addr))
		if canon(got) != canon(want) {
			return fmt.Errorf("Load then Get returned %s at %s", canon(got), addr)
		}
	}
	return loaded.auditValueStore()
}

// hasInvalidUTF8 reports whether a string or map key in v is not valid UTF-8.
func hasInvalidUTF8(v mmdbtype.DataType) bool {
	d, ok := derefValue(v)
	if !ok {
		return false
	}
	switch d := d.(type) {
	case mmdbtype.String:
		return !utf8.ValidString(string(d))
	case mmdbtype.Map:
		for k, c := range d {
			if !utf8.ValidString(string(k)) || hasInvalidUTF8(c) {
				return true
			}
		}
	case mmdbtype.Slice:
		for _, c := range d {
			if hasInvalidUTF8(c) {
				return true
			}
		}
	}
	return false
}

var (
	randomStringLengths = []int{0, 1, 2, 27, 28, 29, 30, 283, 284, 285, 286}
	randomBigLengths    = []int{65820, 65821}
)

// genRandomValue returns a random valid value with sizes and integer widths
// near the MMDB encoding thresholds. big allows strings and bytes of about
// 64 KiB.
func genRandomValue(src *byteSource, depth int, big bool) mmdbtype.DataType {
	kinds := 13
	if depth >= 2 {
		kinds = 11 // No more containers.
	}
	switch src.intn(kinds) {
	case 0:
		return mmdbtype.String(genRandomBytes(src, big))
	case 1:
		return mmdbtype.Bytes(genRandomBytes(src, big))
	case 2:
		edges := []uint64{0, 1, 0xff, 0x100, 0xffff}
		return mmdbtype.Uint16(genRandomUint(src, edges, 0xffff))
	case 3:
		edges := []uint64{0, 1, 0xff, 0x100, 0xffff, 0x10000, 0xffffff, 0x1000000, math.MaxUint32}
		return mmdbtype.Uint32(genRandomUint(src, edges, math.MaxUint32))
	case 4:
		edges := []uint64{0, 1, 0xff, 0x100, math.MaxUint32, 1 << 32, 1 << 56, math.MaxUint64}
		return mmdbtype.Uint64(genRandomUint(src, edges, math.MaxUint64))
	case 5:
		edges := []uint64{0, 1, 0xff, 1 << 63, math.MaxUint64}
		return mmdbtype.Uint128{
			High: genRandomUint(src, edges, math.MaxUint64),
			Low:  genRandomUint(src, edges, math.MaxUint64),
		}
	case 6:
		edges := []int32{0, 1, -1, 127, 128, -128, 255, 256, 0xffff, 0x10000, math.MaxInt32, math.MinInt32}
		return mmdbtype.Int32(edges[src.intn(len(edges))])
	case 7:
		edges := []uint64{
			0, 1 << 63, math.Float64bits(1), math.Float64bits(-1),
			0x7ff8000000000001, 0xfff8000000000002, math.Float64bits(math.Inf(1)),
			math.Float64bits(math.Inf(-1)), 1, math.Float64bits(math.MaxFloat64),
		}
		return mmdbtype.Float64(math.Float64frombits(edges[src.intn(len(edges))]))
	case 8:
		edges := []uint32{
			0, 1 << 31, math.Float32bits(1), 0x7fc00001, 0xffc00002,
			math.Float32bits(float32(math.Inf(1))), 1,
		}
		return mmdbtype.Float32(math.Float32frombits(edges[src.intn(len(edges))]))
	case 9:
		return mmdbtype.Bool(src.byte()&1 == 1)
	case 10:
		return mmdbtype.String(genRandomKey(src))
	case 11:
		sizes := []int{0, 1, 2, 3, 15, 16, 17, 28, 29, 63, 64, 65}
		n := sizes[src.intn(len(sizes))]
		m := make(mmdbtype.Map, n)
		for i := 0; len(m) < n && i < 4*n; i++ {
			key := mmdbtype.String(genRandomKey(src))
			if i >= n {
				key = mmdbtype.String(fmt.Sprintf("%s#%d", key, i))
			}
			m[key] = genRandomValue(src, depth+1+n/16, big)
		}
		return m
	default:
		sizes := []int{0, 1, 2, 3, 28, 29}
		s := make(mmdbtype.Slice, sizes[src.intn(len(sizes))])
		for i := range s {
			s[i] = genRandomValue(src, depth+1+len(s)/16, big)
		}
		return s
	}
}

func genRandomUint(src *byteSource, edges []uint64, maxValue uint64) uint64 {
	if src.byte() < 192 {
		return edges[src.intn(len(edges))]
	}
	var v uint64
	for range 8 {
		v = v<<8 | uint64(src.byte())
	}
	return v & maxValue
}

func genRandomBytes(src *byteSource, big bool) []byte {
	n := randomStringLengths[src.intn(len(randomStringLengths))]
	if big && src.byte() == 255 {
		n = randomBigLengths[src.intn(len(randomBigLengths))]
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = "ab\x00\xff"[src.intn(4)]
	}
	return out
}

func genRandomKey(src *byteSource) string {
	switch c := src.byte(); {
	case c < 160:
		return []string{"", "a", "b", "c", "\xff", "a\x00"}[int(c)%6]
	case c < 224:
		return strings.Repeat("k", randomStringLengths[int(c)%len(randomStringLengths)])
	default:
		return string([]byte{'k', src.byte() % 8, src.byte() % 8})
	}
}

// mutateValue returns a copy of v that differs in one place.
func mutateValue(src *byteSource, v mmdbtype.DataType) mmdbtype.DataType {
	switch v := v.(type) {
	case mmdbtype.Map:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, string(k))
		}
		if len(keys) == 0 || src.byte() < 32 {
			out := v.Copy().(mmdbtype.Map)
			out[mmdbtype.String(fmt.Sprintf("added%d", len(v)))] = mmdbtype.Bool(true)
			return out
		}
		slices.Sort(keys)
		key := mmdbtype.String(keys[src.intn(len(keys))])
		out := v.Copy().(mmdbtype.Map)
		out[key] = mutateValue(src, v[key])
		return out
	case mmdbtype.Slice:
		if len(v) == 0 || src.byte() < 32 {
			return append(v.Copy().(mmdbtype.Slice), mmdbtype.Bool(true))
		}
		i := src.intn(len(v))
		out := v.Copy().(mmdbtype.Slice)
		out[i] = mutateValue(src, v[i])
		return out
	case mmdbtype.String:
		if src.byte() < 64 {
			return mmdbtype.Bytes(v) // Same payload, different type.
		}
		return v + "x"
	case mmdbtype.Bytes:
		return mmdbtype.Bytes(append(bytes.Clone([]byte(v)), 'x'))
	case mmdbtype.Uint16:
		if src.byte() < 64 {
			return mmdbtype.Uint32(v) // Same number, different type.
		}
		return v ^ 1
	case mmdbtype.Uint32:
		if src.byte() < 64 {
			return mmdbtype.Uint64(v)
		}
		return v ^ 1
	case mmdbtype.Uint64:
		return v ^ 1
	case mmdbtype.Uint128:
		return mmdbtype.Uint128{High: v.High, Low: v.Low ^ 1}
	case mmdbtype.Int32:
		return v ^ 1
	case mmdbtype.Float64:
		// Flips the sign: +0 becomes -0, and a NaN keeps its payload.
		return mmdbtype.Float64(math.Float64frombits(math.Float64bits(float64(v)) ^ 1<<63))
	case mmdbtype.Float32:
		return mmdbtype.Float32(math.Float32frombits(math.Float32bits(float32(v)) ^ 1<<31))
	case mmdbtype.Bool:
		return !v
	}
	panic(fmt.Sprintf("cannot mutate %T", v))
}

// scribble changes every container in v in place.
func scribble(v mmdbtype.DataType) {
	switch v := v.(type) {
	case mmdbtype.Map:
		for _, c := range v {
			scribble(c)
		}
		v["scribbled"] = mmdbtype.Bool(true)
	case mmdbtype.Slice:
		for i, c := range v {
			scribble(c)
			if _, ok := c.(mmdbtype.Map); !ok {
				v[i] = mmdbtype.String("scribbled")
			}
		}
	case mmdbtype.Bytes:
		for i := range v {
			v[i] = 'S'
		}
	}
}
