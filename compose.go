package mmdbwriter

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"math/bits"
	"net/netip"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// MergeFunc combines the active value from each composition layer for one
// output network. values has the same length and order as the layers passed to
// Compose; nil marks a layer with no value over the prefix. Returning nil omits
// the output network. The slice and its values are read-only and the slice is
// reused after the function returns.
type MergeFunc func(prefix netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error)

// Compose refines sorted, disjoint source layers into one tree. The merge
// function is called once for each refined output prefix. A nil merge function
// selects the last non-nil layer value.
func Compose(opts Options, layers []NetworkSource, merge MergeFunc) (*Tree, error) {
	tree, err := New(opts)
	if err != nil {
		return nil, err
	}
	if len(layers) == 0 {
		return tree, nil
	}

	cursors := make([]*sourceCursor, len(layers))
	for index, layer := range layers {
		if layer == nil {
			return nil, fmt.Errorf("composition layer %d is nil", index)
		}
		cursor := newSourceCursor(index, layer.Networks())
		cursors[index] = cursor
		defer cursor.stop()
		if err := cursor.pull(); err != nil {
			return nil, err
		}
	}

	position, ok := firstSourcePosition(cursors)
	if !ok {
		return tree, nil
	}
	values := make([]mmdbtype.DataType, len(layers))
	for {
		for _, cursor := range cursors {
			for cursor.hasValue && cursor.interval.end.less(position) {
				if err := cursor.pull(); err != nil {
					return nil, err
				}
			}
		}
		if _, ok := firstSourcePosition(cursors); !ok {
			return tree, nil
		}

		clear(values)
		active := false
		preferIPv4 := false
		var boundary uint128
		hasBoundary := false
		for index, cursor := range cursors {
			if !cursor.hasValue {
				continue
			}
			interval := cursor.interval
			if interval.start.lessOrEqual(position) && position.lessOrEqual(interval.end) {
				active = true
				values[index] = cursor.value.Value
				preferIPv4 = preferIPv4 || interval.ipv4
				if next, exists := interval.end.increment(); exists && (!hasBoundary || next.less(boundary)) {
					boundary = next
					hasBoundary = true
				}
			} else if position.less(interval.start) && (!hasBoundary || interval.start.less(boundary)) {
				boundary = interval.start
				hasBoundary = true
			}
		}

		if !active {
			// Gaps jump directly to the next source start.
			next, exists := firstSourcePosition(cursors)
			if !exists {
				return tree, nil
			}
			position = next
			continue
		}

		end := maxUint128
		if hasBoundary {
			end = boundary.decrement()
		}
		if err := composeRange(tree, position, end, preferIPv4, values, merge); err != nil {
			return nil, err
		}
		if !hasBoundary {
			return tree, nil
		}
		position = boundary
	}
}

func composeRange(
	tree *Tree,
	start uint128,
	end uint128,
	preferIPv4 bool,
	values []mmdbtype.DataType,
	merge MergeFunc,
) error {
	for _, prefix := range prefixesForRange(start, end, preferIPv4) {
		var (
			value mmdbtype.DataType
			err   error
		)
		if merge == nil {
			for index := len(values) - 1; index >= 0; index-- {
				if values[index] != nil {
					value = values[index]
					break
				}
			}
		} else {
			value, err = merge(prefix, values)
			if err != nil {
				return fmt.Errorf("merging network %s: %w", prefix, err)
			}
		}
		if value == nil {
			continue
		}
		if err := tree.Insert(prefix, value); err != nil {
			return fmt.Errorf("inserting composed network %s: %w", prefix, err)
		}
	}
	return nil
}

type uint128 struct {
	hi uint64
	lo uint64
}

var maxUint128 = uint128{hi: math.MaxUint64, lo: math.MaxUint64}

func (u uint128) less(other uint128) bool {
	return u.hi < other.hi || u.hi == other.hi && u.lo < other.lo
}

func (u uint128) lessOrEqual(other uint128) bool { return u == other || u.less(other) }

func (u uint128) increment() (uint128, bool) {
	if u == maxUint128 {
		return uint128{}, false
	}
	result := u
	result.lo++
	if result.lo == 0 {
		result.hi++
	}
	return result, true
}

func (u uint128) decrement() uint128 {
	result := u
	if result.lo == 0 {
		result.hi--
	}
	result.lo--
	return result
}

func (u uint128) trailingZeros() int {
	if u.lo != 0 {
		return bits.TrailingZeros64(u.lo)
	}
	return 64 + bits.TrailingZeros64(u.hi)
}

func (u uint128) blockEnd(hostBits int) uint128 {
	if hostBits == 128 {
		return maxUint128
	}
	result := u
	switch {
	case hostBits == 0:
	case hostBits < 64:
		result.lo |= uint64(1)<<hostBits - 1
	case hostBits == 64:
		result.lo = math.MaxUint64
	default:
		result.lo = math.MaxUint64
		result.hi |= uint64(1)<<(hostBits-64) - 1
	}
	return result
}

func uint128FromPrefix(prefix netip.Prefix) (sourceInterval, error) {
	if !prefix.IsValid() {
		return sourceInterval{}, fmt.Errorf("prefix is invalid")
	}
	prefix = prefix.Masked()
	address := prefix.Addr()
	ipv4 := address.Is4()
	bitsCount := prefix.Bits()
	var raw [16]byte
	if address.Is4In6() {
		if bitsCount < 96 {
			return sourceInterval{}, fmt.Errorf("IPv4-mapped prefix %s is shorter than /96", prefix)
		}
		address = address.Unmap()
		bitsCount -= 96
		ipv4 = true
	}
	if address.Is4() {
		address4 := address.As4()
		copy(raw[12:], address4[:])
		bitsCount += 96
		ipv4 = true
	} else {
		raw = address.As16()
	}
	start := uint128{hi: binary.BigEndian.Uint64(raw[:8]), lo: binary.BigEndian.Uint64(raw[8:])}
	hostBits := 128 - bitsCount
	return sourceInterval{start: start, end: start.blockEnd(hostBits), ipv4: ipv4}, nil
}

func prefixesForRange(start, end uint128, ipv4 bool) []netip.Prefix {
	if start.hi == 0 && end.hi == 0 && end.lo <= math.MaxUint32 {
		ipv4 = true
	}
	prefixes := make([]netip.Prefix, 0, 4)
	for start.lessOrEqual(end) {
		hostBits := start.trailingZeros()
		if ipv4 && hostBits > 32 {
			hostBits = 32
		}
		for start.blockEnd(hostBits).less(end) == false && start.blockEnd(hostBits) != end {
			hostBits--
		}
		if ipv4 {
			address := [4]byte{
				byte(start.lo >> 24), byte(start.lo >> 16), byte(start.lo >> 8), byte(start.lo),
			}
			prefixes = append(prefixes, netip.PrefixFrom(netip.AddrFrom4(address), 32-hostBits))
		} else {
			var address [16]byte
			binary.BigEndian.PutUint64(address[:8], start.hi)
			binary.BigEndian.PutUint64(address[8:], start.lo)
			prefixes = append(prefixes, netip.PrefixFrom(netip.AddrFrom16(address), 128-hostBits))
		}
		blockEnd := start.blockEnd(hostBits)
		next, ok := blockEnd.increment()
		if !ok {
			break
		}
		start = next
	}
	return prefixes
}

type sourceInterval struct {
	start uint128
	end   uint128
	ipv4  bool
}

type sourceCursor struct {
	layer        int
	next         func() (NetworkValue, error, bool)
	stop         func()
	value        NetworkValue
	interval     sourceInterval
	hasValue     bool
	havePrevious bool
	previousEnd  uint128
}

func newSourceCursor(layer int, sequence iter.Seq2[NetworkValue, error]) *sourceCursor {
	next, stop := iter.Pull2(sequence)
	return &sourceCursor{layer: layer, next: next, stop: stop}
}

func (c *sourceCursor) pull() error {
	value, sourceErr, ok := c.next()
	if !ok {
		c.hasValue = false
		return nil
	}
	if sourceErr != nil {
		return fmt.Errorf("reading composition layer %d: %w", c.layer, sourceErr)
	}
	interval, err := uint128FromPrefix(value.Prefix)
	if err != nil {
		return fmt.Errorf("reading composition layer %d network %s: %w", c.layer, value.Prefix, err)
	}
	if c.havePrevious && !c.previousEnd.less(interval.start) {
		return fmt.Errorf(
			"composition layer %d is not sorted and disjoint at network %s",
			c.layer,
			value.Prefix,
		)
	}
	c.value = value
	c.value.Prefix = value.Prefix.Masked()
	c.interval = interval
	c.hasValue = true
	c.havePrevious = true
	c.previousEnd = interval.end
	return nil
}

func firstSourcePosition(cursors []*sourceCursor) (uint128, bool) {
	var first uint128
	found := false
	for _, cursor := range cursors {
		if cursor.hasValue && (!found || cursor.interval.start.less(first)) {
			first = cursor.interval.start
			found = true
		}
	}
	return first, found
}
