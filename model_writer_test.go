package mmdbwriter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// naiveWriter writes a model as an MMDB file. It shares no code with the
// Tree writer. It builds a plain binary trie and makes encoding choices that
// the Tree writer never makes: records that point to pointers, pointers to
// earlier copies of nested values, unsorted and pointed-to map keys, repeated
// copies of a value, integers with leading zero bytes, 4-byte pointers, and
// extra node splits that leave equal neighbor networks. With aliasing, alias
// records point to the IPv4 root node, as the Perl writer does.
type naiveWriter struct {
	src   *byteSource
	model *treeModel
	cfg   modelConfig
	nodes [][2]naiveRecord
	data  bytes.Buffer
	// copies lists the data offsets of the complete copies of each value,
	// by canonical encoding. Pointers may only target these offsets.
	copies   map[string][]int
	ipv4Root int
	// extraSplits bounds the random splits so the trie stays small.
	extraSplits int
	// recordPointers is set when a record points to a pointer. Reader.Verify
	// rejects such a file, because the search tree does not point to the
	// value directly.
	recordPointers bool
	// duplicateKeys is set when a map repeats a key. Load must reject the
	// file.
	duplicateKeys bool
}

type naiveRecordKind int

const (
	naiveEmpty naiveRecordKind = iota
	naiveData
	naiveNode
	naiveAlias
)

type naiveRecord struct {
	kind naiveRecordKind
	// node is the node index for naiveNode, or the data offset for
	// naiveData.
	node int
}

const naiveMaxExtraSplits = 64

// naiveDatabase is a file from the naive writer, with the choices that
// change what a reader or Load must do with it.
type naiveDatabase struct {
	data           []byte
	recordPointers bool
	duplicateKeys  bool
}

func writeNaiveDatabase(src *byteSource, model *treeModel) (naiveDatabase, error) {
	w := &naiveWriter{
		src:      src,
		model:    model,
		cfg:      model.cfg,
		copies:   map[string][]int{},
		ipv4Root: -1,
	}
	root := w.build(u128{}, 0)
	if root.kind != naiveNode {
		return naiveDatabase{}, errors.New("naive writer: the root is not a node")
	}
	if w.cfg.aliasing && w.ipv4Root < 0 {
		return naiveDatabase{}, errors.New("naive writer: no IPv4 root node for aliases")
	}
	data, err := w.encode()
	return naiveDatabase{
		data:           data,
		recordPointers: w.recordPointers,
		duplicateKeys:  w.duplicateKeys,
	}, err
}

// build returns the record for the prefix at start with the given depth.
func (w *naiveWriter) build(start u128, depth int) naiveRecord {
	r := prefixRange(start, depth, w.cfg.depth)
	for _, alias := range w.model.aliases {
		if r == alias.r {
			return naiveRecord{kind: naiveAlias}
		}
	}
	isIPv4Root := w.cfg.depth == 128 && depth == 96 && start == (u128{})
	split := false
	switch {
	case depth == 0, isIPv4Root && w.cfg.aliasing:
		// The root is always a node, and aliases need an IPv4 root node.
		split = true
	case depth == w.cfg.depth:
	case w.containsAliasOrIPv4Root(r, depth):
		split = true
	default:
		if _, ok := w.model.constant(r); !ok {
			split = true
		} else if w.extraSplits < naiveMaxExtraSplits && w.src.byte() < 8 {
			w.extraSplits++
			split = true
		}
	}
	if !split {
		seg := w.model.get(r.first)
		if seg.value == nil {
			return naiveRecord{kind: naiveEmpty}
		}
		return naiveRecord{kind: naiveData, node: w.recordValue(seg.value)}
	}
	index := len(w.nodes)
	w.nodes = append(w.nodes, [2]naiveRecord{})
	if isIPv4Root {
		w.ipv4Root = index
	}
	bit := lowMask(w.cfg.depth - depth).and(lowMask(w.cfg.depth - depth - 1).not())
	left := w.build(start, depth+1)
	right := w.build(start.or(bit), depth+1)
	w.nodes[index] = [2]naiveRecord{left, right}
	return naiveRecord{kind: naiveNode, node: index}
}

// containsAliasOrIPv4Root reports whether r strictly contains an alias, or,
// with aliasing, is an ancestor of the IPv4 root.
func (w *naiveWriter) containsAliasOrIPv4Root(r mrange, depth int) bool {
	for _, alias := range w.model.aliases {
		if alias.r.within(r) && alias.r != r {
			return true
		}
	}
	return w.cfg.aliasing && depth < 96 && r.first == (u128{})
}

// recordValue writes or reuses a value for a data record and returns the
// offset the record points to. Sometimes that offset holds a pointer.
func (w *naiveWriter) recordValue(v mmdbtype.DataType) int {
	choice := w.src.byte()
	var offset int
	if copies := w.copies[canon(v)]; len(copies) > 0 && choice < 128 {
		offset = copies[int(choice)%len(copies)]
	} else {
		offset = w.writeValue(v)
	}
	if choice&3 == 1 {
		w.recordPointers = true
		pointer := w.data.Len()
		w.writePointer(offset)
		return pointer
	}
	return offset
}

// writeChild writes a nested value inline or as a pointer to an earlier copy.
func (w *naiveWriter) writeChild(v mmdbtype.DataType) {
	if copies := w.copies[canon(v)]; len(copies) > 0 && w.src.byte() < 128 {
		w.writePointer(copies[len(copies)-1])
		return
	}
	w.writeValue(v)
}

const (
	mmdbTypePointer = 1
	mmdbTypeString  = 2
	mmdbTypeDouble  = 3
	mmdbTypeBytes   = 4
	mmdbTypeUint16  = 5
	mmdbTypeUint32  = 6
	mmdbTypeMap     = 7
	mmdbTypeInt32   = 8
	mmdbTypeUint64  = 9
	mmdbTypeUint128 = 10
	mmdbTypeArray   = 11
	mmdbTypeBool    = 14
	mmdbTypeFloat   = 15
)

// writeValue writes a complete copy of v and returns its offset.
func (w *naiveWriter) writeValue(v mmdbtype.DataType) int {
	offset := w.data.Len()
	switch v := v.(type) {
	case mmdbtype.Map:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, string(k))
		}
		slices.Sort(keys)
		// Shuffle, so keys are often out of order.
		for i := len(keys) - 1; i > 0; i-- {
			j := int(w.src.byte()) % (i + 1)
			keys[i], keys[j] = keys[j], keys[i]
		}
		if len(keys) > 0 && w.src.byte() == 255 {
			// Repeat a key with the same value. A reader that keeps one
			// entry decodes the same map, but Load must reject it.
			w.duplicateKeys = true
			keys = append(keys, keys[0])
		}
		w.writeControl(mmdbTypeMap, len(keys))
		for _, k := range keys {
			w.writeChild(mmdbtype.String(k))
			w.writeChild(v[mmdbtype.String(k)])
		}
	case mmdbtype.Slice:
		w.writeControl(mmdbTypeArray, len(v))
		for _, e := range v {
			w.writeChild(e)
		}
	case mmdbtype.String:
		w.writeControl(mmdbTypeString, len(v))
		w.data.WriteString(string(v))
	case mmdbtype.Bytes:
		w.writeControl(mmdbTypeBytes, len(v))
		w.data.Write(v)
	case mmdbtype.Float64:
		w.writeControl(mmdbTypeDouble, 8)
		w.data.Write(binary.BigEndian.AppendUint64(nil, math.Float64bits(float64(v))))
	case mmdbtype.Float32:
		w.writeControl(mmdbTypeFloat, 4)
		w.data.Write(binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(v))))
	case mmdbtype.Bool:
		size := 0
		if v {
			size = 1
		}
		w.writeControl(mmdbTypeBool, size)
	case mmdbtype.Uint16:
		w.writeUint(mmdbTypeUint16, 2, 0, uint64(v))
	case mmdbtype.Uint32:
		w.writeUint(mmdbTypeUint32, 4, 0, uint64(v))
	case mmdbtype.Uint64:
		w.writeUint(mmdbTypeUint64, 8, 0, uint64(v))
	case mmdbtype.Uint128:
		w.writeUint(mmdbTypeUint128, 16, v.High, v.Low)
	case mmdbtype.Int32:
		if v < 0 {
			w.writeControl(mmdbTypeInt32, 4)
			w.data.Write(binary.BigEndian.AppendUint32(nil, uint32(v)))
		} else {
			w.writeUint(mmdbTypeInt32, 4, 0, uint64(v))
		}
	default:
		panic(fmt.Sprintf("naive writer: unsupported value %T", v))
	}
	key := canon(v)
	w.copies[key] = append(w.copies[key], offset)
	return offset
}

// writeUint writes an unsigned integer with the fewest bytes, or sometimes
// with leading zero bytes up to the type's width.
func (w *naiveWriter) writeUint(typeNum, width int, hi, lo uint64) {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], hi)
	binary.BigEndian.PutUint64(buf[8:], lo)
	encoded := buf[16-width:]
	for len(encoded) > 0 && encoded[0] == 0 {
		encoded = encoded[1:]
	}
	if pad := int(w.src.byte()) % 4; pad > 0 && w.src.byte() < 64 {
		start := max(0, 16-width, 16-len(encoded)-pad)
		encoded = buf[start:]
	}
	w.writeControl(typeNum, len(encoded))
	w.data.Write(encoded)
}

func (w *naiveWriter) writeControl(typeNum, size int) {
	var first byte
	extended := typeNum > 7
	if !extended {
		first = byte(typeNum) << 5
	}
	var sizeBytes []byte
	switch {
	case size < 29:
		first |= byte(size)
	case size < 29+256:
		first |= 29
		sizeBytes = []byte{byte(size - 29)}
	case size < 285+65536:
		first |= 30
		sizeBytes = binary.BigEndian.AppendUint16(nil, uint16(size-285))
	default:
		first |= 31
		rest := size - 65821
		sizeBytes = []byte{byte(rest >> 16), byte(rest >> 8), byte(rest)}
	}
	w.data.WriteByte(first)
	if extended {
		w.data.WriteByte(byte(typeNum - 7))
	}
	w.data.Write(sizeBytes)
}

// writePointer writes a pointer to a data offset. It uses the shortest form,
// or sometimes the 4-byte form.
func (w *naiveWriter) writePointer(offset int) {
	const ptr = mmdbTypePointer << 5
	switch {
	case w.src.byte() < 32:
		w.data.WriteByte(ptr | 3<<3)
		w.data.Write(binary.BigEndian.AppendUint32(nil, uint32(offset)))
	case offset < 1<<11:
		w.data.Write([]byte{ptr | byte(offset>>8), byte(offset)})
	case offset < 2048+1<<19:
		v := offset - 2048
		w.data.Write([]byte{ptr | 1<<3 | byte(v>>16), byte(v >> 8), byte(v)})
	case offset < 526336+1<<27:
		v := offset - 526336
		w.data.Write([]byte{ptr | 2<<3 | byte(v>>24), byte(v >> 16), byte(v >> 8), byte(v)})
	default:
		w.data.WriteByte(ptr | 3<<3)
		w.data.Write(binary.BigEndian.AppendUint32(nil, uint32(offset)))
	}
}

func (w *naiveWriter) encode() ([]byte, error) {
	nodeCount := len(w.nodes)
	recordValue := func(r naiveRecord) int {
		switch r.kind {
		case naiveNode:
			return r.node
		case naiveAlias:
			return w.ipv4Root
		case naiveData:
			return nodeCount + 16 + r.node
		}
		return nodeCount
	}
	maxValue := nodeCount + 16 + w.data.Len()
	recordSize := 0
	for _, size := range []int{24, 28, 32} {
		if maxValue < 1<<size && (recordSize == 0 || w.src.byte() < 128) {
			recordSize = size
		}
	}
	if recordSize == 0 {
		return nil, errors.New("naive writer: the database is too large")
	}
	var out bytes.Buffer
	for _, n := range w.nodes {
		left, right := recordValue(n[0]), recordValue(n[1])
		switch recordSize {
		case 24:
			out.Write([]byte{
				byte(left >> 16), byte(left >> 8), byte(left),
				byte(right >> 16), byte(right >> 8), byte(right),
			})
		case 28:
			out.Write([]byte{
				byte(left >> 16), byte(left >> 8), byte(left),
				byte(left>>24&0x0f<<4 | right>>24&0x0f),
				byte(right >> 16), byte(right >> 8), byte(right),
			})
		case 32:
			out.Write(binary.BigEndian.AppendUint32(nil, uint32(left)))
			out.Write(binary.BigEndian.AppendUint32(nil, uint32(right)))
		}
	}
	out.Write(make([]byte, 16))
	out.Write(w.data.Bytes())
	out.WriteString("\xAB\xCD\xEFMaxMind.com")

	ipVersion := 6
	if w.cfg.depth == 32 {
		ipVersion = 4
	}
	meta := &naiveWriter{src: &byteSource{}, copies: map[string][]int{}}
	meta.writeValue(mmdbtype.Map{
		"binary_format_major_version": mmdbtype.Uint16(2),
		"binary_format_minor_version": mmdbtype.Uint16(0),
		"build_epoch":                 mmdbtype.Uint64(1),
		"database_type":               mmdbtype.String("naive-model-test"),
		"description":                 mmdbtype.Map{"en": mmdbtype.String("Naive model test")},
		"ip_version":                  mmdbtype.Uint16(ipVersion),
		"languages":                   mmdbtype.Slice{},
		"node_count":                  mmdbtype.Uint32(nodeCount),
		"record_size":                 mmdbtype.Uint16(recordSize),
	})
	out.Write(meta.data.Bytes())
	return out.Bytes(), nil
}
