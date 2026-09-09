package mmdbwriter

import (
	"errors"
	"fmt"
	"iter"
	"net/netip"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/internal/treeaddr"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// NetworkValue associates an MMDB value with a network prefix.
type NetworkValue struct {
	Prefix netip.Prefix
	Value  mmdbtype.DataType
}

// NetworkSource produces masked, ascending, disjoint networks. Yielded values
// must remain valid and unchanged throughout enumeration. Call Copy before
// modifying a value. Repeatability is source-specific. IPv4 sorts within ::/96
// in tree address order, including normalized IPv4-mapped prefixes.
type NetworkSource interface {
	Networks() iter.Seq2[NetworkValue, error]
}

type sequenceSource struct {
	sequence iter.Seq2[NetworkValue, error]
}

// SourceFunc adapts an already sorted, disjoint sequence to NetworkSource.
// Its repeatability and value ownership follow the supplied sequence.
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
// The caller must keep the reader open throughout enumeration. Each enumeration
// creates a separate cache. Aliases are omitted by default. Including aliases
// can produce overlapping prefixes after normalization, which Compose rejects.
// For files built with IPv4 aliasing disabled, pass IncludeAliasedNetworks to
// preserve records the reader would otherwise treat as aliases.
func MMDBSource(reader *maxminddb.Reader, options ...maxminddb.NetworksOption) NetworkSource {
	return &mmdbSource{reader: reader, options: append([]maxminddb.NetworksOption(nil), options...)}
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
// networks. Values are shared, read-only store-materialized views. Call Copy
// before modifying one. Reserved, empty, and alias records are omitted.
// Enumeration is repeatable. Do not mutate or concurrently access the tree
// during enumeration.
func (t *Tree) Networks() iter.Seq2[NetworkValue, error] {
	return func(yield func(NetworkValue, error) bool) {
		if t == nil {
			yield(NetworkValue{}, errors.New("tree source is nil"))
			return
		}
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
		prefix, err := treeaddr.PrefixFromInsertIP(
			ip,
			depth,
			t.treeDepth,
			depth >= 96 && treeaddr.IsIPv4SubtreeIP(ip),
		)
		if err != nil {
			yield(NetworkValue{}, err)
			return false
		}
		return yield(NetworkValue{
			Prefix: prefix,
			Value:  t.valueStore.materialize(record.value),
		}, nil)
	case recordTypeNode, recordTypeFixedNode:
		node := t.nodeAt(record.nodeIndex)
		for side := range 2 {
			childIP := ip
			setBitAt(&childIP, depth, byte(side))
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
		yield(NetworkValue{}, fmt.Errorf(
			"enumerating record type %d is not implemented",
			record.recordType,
		))
		return false
	}
	return true
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
	sequence := source.Networks()
	if sequence == nil {
		return errors.New("network source has a nil sequence")
	}
	initialLength := len(s.values)
	for value, err := range sequence {
		if err != nil {
			clear(s.values[initialLength:])
			s.values = s.values[:initialLength]
			return err
		}
		if err := s.Add(value); err != nil {
			clear(s.values[initialLength:])
			s.values = s.values[:initialLength]
			return err
		}
	}
	return nil
}

// Networks sorts and resolves all values added to the source, then yields
// ascending disjoint networks. Enumeration is repeatable and rebuilds the
// temporary tree each time. Do not modify the source during enumeration.
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
