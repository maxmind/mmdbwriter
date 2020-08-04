// Package mmdbwriter provides the tools to create and write MaxMind DB
// files.
package mmdbwriter

import (
	"bufio"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
)

var (
	metadataStartMarker  = []byte("\xAB\xCD\xEFMaxMind.com")
	dataSectionSeparator = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
)

// Tree represents an MaxMind DB search tree.
type Tree struct {
	buildEpoch   int64
	databaseType string
	description  map[string]string
	ipVersion    int
	languages    []string
	recordSize   int
	root         *node
	treeDepth    int
	// This is set when the tree is finalized
	nodeCount int
}

// New creates a new Tree.
func New() *Tree {
	return &Tree{
		// TODO: allow setting of many of these
		buildEpoch:   time.Now().Unix(),
		databaseType: "Test",
		description:  map[string]string{},
		ipVersion:    6,
		languages:    []string{},
		recordSize:   32,
		root:         &node{},
		// TODO: support IPv4 trees
		treeDepth: 128,
	}
}

// Insert a data value into the tree.
func (t *Tree) Insert(
	network *net.IPNet,
	// XXX - temp
	value string,
) error {
	// We set this to 0 so that the tree must be finalized again.
	t.nodeCount = 0

	prefixLen, _ := network.Mask.Size()

	if prefixLen == 0 {
		// It isn't possible to do this as there isn't a record for the root node.
		// If we wanted to support this, we would have to divide it into two /1
		// insertions, but there isn't a reason to bother supporting it.
		return errors.New("cannot insert a value into the root node of the tree")
	}

	t.root.insert(network.IP, prefixLen, 0, value)
	return nil
}

// Get the value for the given IP address from the tree.
func (t *Tree) Get(ip net.IP) (*net.IPNet, *string) {
	if t.treeDepth == 128 && len(ip) == 4 {
		ip = ipV4ToV6(ip)
	}

	prefixLen, value := t.root.get(ip, 0)
	mask := net.CIDRMask(prefixLen, t.treeDepth)

	return &net.IPNet{
		IP:   ip.Mask(mask),
		Mask: mask,
	}, value
}

// Finalize prepares the tree for writing. It is not threadsafe.
func (t *Tree) Finalize() {
	t.nodeCount = t.root.finalize(0)
}

// WriteTo writes the tree to the provided Writer.
func (t *Tree) WriteTo(w io.Writer) (int64, error) {
	if t.nodeCount == 0 {
		return 0, errors.New("the Tree is not finalized; run Finalize() before writing")
	}

	buf := bufio.NewWriter(w)

	// We create this here so that we don't have to allocate millions of these. This
	// may no longer make sense now that we are using a bufio.Writer anyway, which has
	// WriteByte, but we should probably do some testing.
	recordBuf := make([]byte, 2*t.recordSize/8)

	nodeCount, numBytes, err := t.writeNode(buf, t.root, recordBuf)
	if nodeCount != t.nodeCount {
		_ = buf.Flush()
		// This should only happen if there is a programming bug
		// in this library.
		return numBytes, errors.Errorf(
			"number of nodes written (%d) doesn't match number expected (%d)",
			nodeCount,
			t.nodeCount,
		)
	}

	nb, err := buf.Write(dataSectionSeparator)
	numBytes += int64(nb)
	if err != nil {
		_ = buf.Flush()
		return numBytes, errors.Wrap(err, "error writing data section separator")
	}

	nb, err = buf.Write(metadataStartMarker)
	numBytes += int64(nb)
	if err != nil {
		_ = buf.Flush()
		return numBytes, errors.Wrap(err, "error writing metadata start marker")
	}

	nb64, err := t.writeMetadata(buf)
	numBytes += nb64
	if err != nil {
		_ = buf.Flush()
		return numBytes, errors.Wrap(err, "error writing metadata")
	}

	err = buf.Flush()
	if err != nil {
		return numBytes, errors.Wrap(err, "error flushing buffer to writer")
	}

	return numBytes, err
}

func (t *Tree) writeNode(
	w io.Writer,
	n *node,
	recordBuf []byte,
) (int, int64, error) {
	if n.isLeaf() {
		return 0, 0, nil
	}

	err := t.copyRecord(recordBuf, n.children)
	if err != nil {
		return 0, 0, err
	}

	numBytes := int64(0)
	nb, err := w.Write(recordBuf)
	numBytes += int64(nb)
	nodesWritten := 1
	if err != nil {
		return nodesWritten, numBytes, errors.Wrap(err, "error writing node")
	}

	leftNodes, leftNumBytes, err := t.writeNode(
		w,
		n.children[0],
		recordBuf,
	)
	nodesWritten += leftNodes
	numBytes += leftNumBytes
	if err != nil {
		return nodesWritten, numBytes, err
	}

	rightNodes, rightNumBytes, err := t.writeNode(
		w,
		n.children[1],
		recordBuf,
	)
	nodesWritten += rightNodes
	numBytes += rightNumBytes
	return nodesWritten, numBytes, err
}

func (t *Tree) recordValueForNode(
	n *node,
) int {
	if n.isLeaf() {
		// XXX - support data nodes !!
		return t.nodeCount
	}
	return n.nodeNum
}

func (t *Tree) copyRecord(buf []byte, children [2]*node) error {
	left := t.recordValueForNode(children[0])
	right := t.recordValueForNode(children[1])

	// XXX check max size

	switch t.recordSize {
	case 24:
		buf[0] = byte((left >> 16) & 0xFF)
		buf[1] = byte((left >> 8) & 0xFF)
		buf[2] = byte(left & 0xFF)
		buf[3] = byte((right >> 16) & 0xFF)
		buf[4] = byte((right >> 8) & 0xFF)
		buf[5] = byte(right & 0xFF)
	case 28:
		buf[0] = byte((left >> 16) & 0xFF)
		buf[1] = byte((left >> 8) & 0xFF)
		buf[2] = byte(left & 0xFF)
		buf[3] = byte((((left >> 24) & 0x0F) << 4) | (right >> 24 & 0x0F))
		buf[4] = byte((right >> 16) & 0xFF)
		buf[5] = byte((right >> 8) & 0xFF)
		buf[6] = byte(right & 0xFF)
	case 32:
		buf[0] = byte((left >> 24) & 0xFF)
		buf[1] = byte((left >> 16) & 0xFF)
		buf[2] = byte((left >> 8) & 0xFF)
		buf[3] = byte(left & 0xFF)
		buf[4] = byte((right >> 24) & 0xFF)
		buf[5] = byte((right >> 16) & 0xFF)
		buf[6] = byte((right >> 8) & 0xFF)
		buf[7] = byte(right & 0xFF)
	default:
		return errors.Errorf("unsupported record size of %d", t.recordSize)
	}
	return nil
}

var v4Prefix = net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

func ipV4ToV6(ip net.IP) net.IP {
	return append(v4Prefix, ip...)
}

func (t *Tree) writeMetadata(w *bufio.Writer) (int64, error) {
	description := typeMap{}
	for k, v := range t.description {
		description[typeString(k)] = typeString(v)
	}

	languages := typeSlice{}
	for _, v := range t.languages {
		languages = append(languages, typeString(v))
	}
	metadata := typeMap{
		"binary_format_major_version": typeUint16(2),
		"binary_format_minor_version": typeUint16(0),
		"build_epoch":                 typeUint64(t.buildEpoch),
		"database_type":               typeString(t.databaseType),
		"description":                 description,
		"ip_version":                  typeUint16(t.ipVersion),
		"languages":                   languages,
		"node_count":                  typeUint32(t.nodeCount),
		"record_size":                 typeUint16(t.recordSize),
	}
	return metadata.writeTo(w)
}
