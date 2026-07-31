package mmdbwriter

import (
	"errors"
	"fmt"
	"iter"
	"net/netip"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// NetworkValue associates an MMDB value with a network prefix.
type NetworkValue struct {
	Prefix netip.Prefix
	Value  mmdbtype.DataType
}

// NetworkSource produces masked, ascending, disjoint networks. Whether a
// source can be enumerated more than once is source-specific.
type NetworkSource interface {
	Networks() iter.Seq2[NetworkValue, error]
}

type sequenceSource struct {
	sequence iter.Seq2[NetworkValue, error]
}

// SourceFunc adapts an already sorted, disjoint sequence to NetworkSource.
func SourceFunc(sequence iter.Seq2[NetworkValue, error]) NetworkSource {
	return sequenceSource{sequence: sequence}
}

func (s sequenceSource) Networks() iter.Seq2[NetworkValue, error] {
	if s.sequence == nil {
		return func(yield func(NetworkValue, error) bool) {
			yield(NetworkValue{}, errors.New("network sequence is nil"))
		}
	}
	return s.sequence
}

type mmdbSource struct {
	reader  *maxminddb.Reader
	options []maxminddb.NetworksOption
}

// MMDBSource adapts a maxminddb Reader to NetworkSource. Values are decoded
// lazily and cached by data offset for the duration of each enumeration. A
// yielded value may be shared by multiple records and must be treated as
// read-only. Call Copy before modifying it.
func MMDBSource(reader *maxminddb.Reader, options ...maxminddb.NetworksOption) NetworkSource {
	return &mmdbSource{reader: reader, options: options}
}

func (s *mmdbSource) Networks() iter.Seq2[NetworkValue, error] {
	return func(yield func(NetworkValue, error) bool) {
		if s == nil || s.reader == nil {
			yield(NetworkValue{}, errors.New("MMDB source has a nil reader"))
			return
		}
		unmarshaler := mmdbtype.NewUnmarshaler()
		byOffset := map[uintptr]mmdbtype.DataType{}
		for result := range s.reader.Networks(s.options...) {
			prefix := result.Prefix()
			if err := result.Err(); err != nil {
				yield(NetworkValue{}, fmt.Errorf("reading network %s: %w", prefix, err))
				return
			}
			value, ok := byOffset[result.Offset()]
			if !ok {
				unmarshaler.Clear()
				if err := result.Decode(unmarshaler); err != nil {
					yield(NetworkValue{}, fmt.Errorf("decoding network %s: %w", prefix, err))
					return
				}
				value = unmarshaler.Result()
				byOffset[result.Offset()] = value
			}
			if !yield(NetworkValue{Prefix: prefix, Value: value}, nil) {
				return
			}
		}
	}
}

// Networks enumerates the Tree's data records as ascending, disjoint
// networks. Values are shared, read-only store-materialized views; call Copy
// before modifying one. Reserved, empty, and alias records are omitted.
func (t *Tree) Networks() iter.Seq2[NetworkValue, error] {
	return func(yield func(NetworkValue, error) bool) {
		var ip [16]byte
		t.walkNetworks(record{nodeIndex: t.root, recordType: recordTypeNode}, ip, 0, yield)
	}
}

func (t *Tree) walkNetworks(
	record record,
	ip [16]byte,
	depth int,
	yield func(NetworkValue, error) bool,
) bool {
	switch record.recordType {
	case recordTypeData:
		prefix := t.internalPrefix(ip, depth)
		return yield(NetworkValue{
			Prefix: prefix,
			Value:  t.valueStore.materialize(record.value),
		}, nil)
	case recordTypeNode, recordTypeFixedNode:
		node := t.nodeAt(record.nodeIndex)
		for side := range 2 {
			childIP := ip
			setBit(&childIP, depth, byte(side))
			if !t.walkNetworks(node.children[side], childIP, depth+1, yield) {
				return false
			}
		}
	case recordTypePath:
		path := t.paths[record.nodeIndex]
		return t.walkNetworks(path.record, path.ip, path.endDepth, yield)
	case recordTypeEmpty, recordTypeReserved, recordTypeAlias:
		return true
	default:
		return yield(NetworkValue{}, fmt.Errorf(
			"enumerating record type %d is not implemented",
			record.recordType,
		))
	}
	return true
}

func (t *Tree) internalPrefix(ip [16]byte, depth int) netip.Prefix {
	if t.treeDepth == 32 {
		var address [4]byte
		copy(address[:], ip[:4])
		return netip.PrefixFrom(netip.AddrFrom4(address), depth)
	}
	if depth >= 96 {
		allZero := true
		for _, value := range ip[:12] {
			if value != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			var address [4]byte
			copy(address[:], ip[12:])
			return netip.PrefixFrom(netip.AddrFrom4(address), depth-96)
		}
	}
	return netip.PrefixFrom(netip.AddrFrom16(ip), depth)
}

func setBit(ip *[16]byte, depth int, value byte) {
	mask := byte(1 << (7 - depth%8))
	if value == 0 {
		ip[depth/8] &^= mask
	} else {
		ip[depth/8] |= mask
	}
}

// SortingSource accepts networks in any order and resolves overlaps according
// to insertion order. Call Insert or Add to populate it before enumeration.
type SortingSource struct {
	resolve inserter.Func
	values  []NetworkValue
}

// NewSortingSource creates an unsorted source. A nil resolve function means
// inserter.Replace.
func NewSortingSource(resolve inserter.Func) *SortingSource {
	return &SortingSource{resolve: resolve}
}

// Insert adds a value to the unsorted source. The value must not be modified
// after insertion.
func (s *SortingSource) Insert(prefix netip.Prefix, value mmdbtype.DataType) error {
	if !prefix.IsValid() {
		return errors.New("prefix is invalid")
	}
	if prefix.Addr().Is4In6() {
		if prefix.Bits() < 96 {
			return errors.New("IPv4-mapped prefixes shorter than /96 cannot be inserted")
		}
		prefix = netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96)
	}
	s.values = append(s.values, NetworkValue{Prefix: prefix.Masked(), Value: value})
	return nil
}

// Add adds a NetworkValue to the unsorted source.
func (s *SortingSource) Add(value NetworkValue) error {
	return s.Insert(value.Prefix, value.Value)
}

// AddSource consumes another source in its yielded order.
func (s *SortingSource) AddSource(source NetworkSource) error {
	if source == nil {
		return errors.New("network source is nil")
	}
	initialLength := len(s.values)
	for value, err := range source.Networks() {
		if err != nil {
			s.values = s.values[:initialLength]
			return err
		}
		if err := s.Add(value); err != nil {
			s.values = s.values[:initialLength]
			return err
		}
	}
	return nil
}

// Networks sorts and resolves all values added to the source, then yields
// ascending disjoint networks.
func (s *SortingSource) Networks() iter.Seq2[NetworkValue, error] {
	return func(yield func(NetworkValue, error) bool) {
		tree, err := New(Options{
			DisableIPv4Aliasing:     true,
			IncludeReservedNetworks: true,
		})
		if err != nil {
			yield(NetworkValue{}, err)
			return
		}
		for _, value := range s.values {
			if s.resolve == nil {
				err = tree.Insert(value.Prefix, value.Value)
			} else {
				err = tree.InsertFunc(value.Prefix, value.Value, s.resolve)
			}
			if err != nil {
				yield(NetworkValue{}, err)
				return
			}
		}
		for value, networkErr := range tree.Networks() {
			if !yield(value, networkErr) {
				return
			}
		}
	}
}
