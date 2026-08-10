// Package mmdbwriter provides the tools to create and write MaxMind DB
// files.
package mmdbwriter

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net/netip"
	"os"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

var (
	metadataStartMarker  = []byte("\xAB\xCD\xEFMaxMind.com")
	dataSectionSeparator = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
)

// Options holds configuration parameters for the writer.
type Options struct {
	// BuildEpoch is the database build timestamp as a Unix epoch value. It
	// defaults to the epoch of when New was called.
	BuildEpoch int64

	// DatabaseType is a string that indicates the structure of each data record
	// associated with an IP address. The actual definition of these structures
	// is left up to the database creator.
	DatabaseType string

	// Description is a map where the key is a language code and the value is
	// the description of the database in that language.
	Description map[string]string

	// DisableIPv4Aliasing will disable the IPv4 aliasing in IPv6 trees. This
	// aliasing maps some IPv6 networks to the IPv4 network, e.g.,
	// ::ffff:0:0/96.
	DisableIPv4Aliasing bool

	// IncludeReservedNetworks will allow reserved networks to be added to the
	// database.
	//
	// If this is false, any attempt to insert into these networks will result
	// in an error and inserting a network that contains a reserved network will
	// result in the reserved portion of the network being excluded. Reserved
	// networks that are globally routable to an individual device, such as
	// Teredo, may still be added.
	IncludeReservedNetworks bool

	// IPVersion indicates whether an IPv4 or IPv6 database should be built. An
	// IPv6 database supports both IPv4 and IPv6 lookups. The default value is
	// "6" for IPv6.
	IPVersion int

	// Languages is a slice of strings, each of which is a locale code. A given
	// record may contain data items that have been localized to some or all of
	// these locales. Records should not contain localized data for locales not
	// included in this slice.
	Languages []string

	// RecordSize indicates the number of bits in a record in the search tree.
	// The supported values are 24, 28, and 32. A smaller size will result in a
	// smaller database, but it will limit the maximum size of the database.
	// The default is 28.
	RecordSize int

	// DisableMetadataPointers prevents the use of pointers in the metadata
	// section of the database. This option exists to avoid bugs in reader
	// implementations that do not correctly handle metadata pointers. Its
	// use should primarily be limited to existing database types.
	DisableMetadataPointers bool

	// Inserter is the function used when calling `Insert`. Leaving it nil
	// is equivalent to `inserter.Replace`, which replaces any conflicting old
	// value entirely with the new, and allows Insert to use the default
	// direct-value fast path. Passing `inserter.Replace` explicitly has the same
	// behavior but skips that optimization.
	//
	// An Inserter must not modify either argument. The existing value is a
	// shared, read-only view. It is equal to, but not necessarily the same
	// object as, the value originally inserted. The new value is the value
	// passed to the insert call, or a shared view of the decoded record
	// during Load. Nested containers can be shared across records, so copy a
	// value before you modify it. The tree calls the function separately for
	// every covered record. Any non-nil returned value becomes tree-owned and
	// must not be modified after return.
	Inserter inserter.Func
}

// Tree represents an MaxMind DB search tree. A Tree is not safe for
// concurrent use. Lookups materialize shared views lazily, so the caller must
// synchronize even concurrent Get calls.
type Tree struct {
	buildEpoch              int64
	databaseType            string
	valueStore              *valueStore
	description             map[string]string
	disableMetadataPointers bool
	ipVersion               int
	languages               []string
	recordSize              int
	// nodeBlocks is an append-only arena split into fixed-size blocks. Blocks
	// preserve pointer stability during inserts but grow monotonically; merged
	// or abandoned nodes are not reclaimed until the Tree is discarded.
	nodeBlocks         [][]node
	nodeCountAllocated int
	// nodeNumbers and nodeCount are invalidated by mutation and rebuilt lazily
	// by finalize before writing.
	nodeNumbers []int
	// paths is an append-only arena for compressed sparse insertion paths. Path
	// entries are not reclaimed after materialization.
	paths     []compressedPath
	root      nodeIndex
	treeDepth int

	nodeCount int
	inserter  inserter.Func
	// refcountAudit runs the full ownership audit after every successful
	// insert and load. It is read from MMDBWRITER_REFCOUNT_AUDIT in New.
	refcountAudit bool
}

// New creates a new Tree.
func New(opts Options) (*Tree, error) {
	tree := &Tree{
		buildEpoch:              time.Now().Unix(),
		databaseType:            opts.DatabaseType,
		description:             map[string]string{},
		disableMetadataPointers: opts.DisableMetadataPointers,
		ipVersion:               6,
		recordSize:              28,
		nodeBlocks:              [][]node{make([]node, nodeBlockSize)},
		nodeCountAllocated:      1,
		root:                    rootNodeIndex,
		refcountAudit:           os.Getenv("MMDBWRITER_REFCOUNT_AUDIT") != "",
	}

	if opts.BuildEpoch != 0 {
		tree.buildEpoch = opts.BuildEpoch
	}

	if opts.Description != nil {
		tree.description = opts.Description
	}

	if opts.IPVersion != 0 {
		tree.ipVersion = opts.IPVersion
	}

	tree.valueStore = newValueStore()

	if opts.Languages != nil {
		tree.languages = opts.Languages
	}

	if opts.RecordSize != 0 {
		tree.recordSize = opts.RecordSize
	}

	if opts.Inserter != nil {
		tree.inserter = opts.Inserter
	}

	switch tree.ipVersion {
	case 6:
		tree.treeDepth = 128
	case 4:
		tree.treeDepth = 32
	default:
		return nil, fmt.Errorf("unsupported IPVersion: %d", tree.ipVersion)
	}

	switch tree.recordSize {
	case 24, 28, 32:
	default:
		return nil, fmt.Errorf("unsupported RecordSize: %d", tree.recordSize)
	}

	if tree.buildEpoch < 0 {
		return nil, fmt.Errorf("BuildEpoch must not be negative: %d", tree.buildEpoch)
	}

	if tree.ipVersion == 6 && !opts.DisableIPv4Aliasing {
		if err := tree.insertIPv4Aliases(); err != nil {
			return nil, err
		}
	}

	if !opts.IncludeReservedNetworks {
		err := tree.insertReservedNetworks()
		if err != nil {
			return nil, err
		}
	}

	return tree, nil
}

// metadataDimension narrows a search tree dimension read from metadata. New
// validates which values are supported; this rejects two cases it cannot see.
// Zero is rejected because it is indistinguishable from an unset Option and
// would be silently replaced by a default. Values above math.MaxInt32 are
// rejected because the uint to int conversion is otherwise unchecked.
func metadataDimension(name string, value uint) (int, error) {
	if value == 0 || value > math.MaxInt32 {
		return 0, fmt.Errorf("unsupported %s in metadata: %d", name, value)
	}
	return int(value), nil
}

// Load loads an existing database into the writer. It interns records into
// the value store directly from the database, without building intermediate
// map and slice graphs. Source networks that share a data offset share one
// stored value.
// During the load, a cache holds one reference per distinct offset in the
// source data section. Load releases the cache before it returns. A
// non-nil Options.Inserter instead receives a materialized view of each
// decoded record. The inserter must treat its arguments as immutable and must
// copy a value before modifying it.
func Load(path string, opts Options) (*Tree, error) {
	db, err := maxminddb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer db.Close()

	metadata := db.Metadata
	if opts.DatabaseType == "" {
		opts.DatabaseType = metadata.DatabaseType
	}

	if opts.Description == nil {
		opts.Description = metadata.Description
	}

	if opts.IPVersion == 0 {
		opts.IPVersion, err = metadataDimension("ip_version", metadata.IPVersion)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}
	}

	if opts.Languages == nil {
		opts.Languages = metadata.Languages
	}

	if opts.RecordSize == 0 {
		opts.RecordSize, err = metadataDimension("record_size", metadata.RecordSize)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}
	}

	tree, err := New(opts)
	if err != nil {
		return nil, fmt.Errorf("creating tree for %s: %w", path, err)
	}

	// The decoder interns records straight into the value store. It caches
	// one reference per source data offset, so shared records decode once.
	decoder := newStoreDecoder(tree.valueStore)
	defer decoder.close()

	var networkOpts []maxminddb.NetworksOption
	if opts.IPVersion == 6 && opts.DisableIPv4Aliasing {
		networkOpts = append(networkOpts, maxminddb.IncludeAliasedNetworks())
	}

	for res := range db.Networks(networkOpts...) {
		prefix := res.Prefix()
		if err := res.Err(); err != nil {
			return nil, fmt.Errorf("loading network %s: %w", prefix, err)
		}

		if err := res.Decode(decoder); err != nil {
			return nil, fmt.Errorf("unmarshaling record for network %s: %w", prefix, err)
		}
		value := decoder.takeResult()

		prefix, err := tree.normalizeLoadPrefix(prefix)
		if err != nil {
			tree.valueStore.release(value)
			return nil, err
		}

		err = tree.insertNormalizedRef(
			prefix,
			recordTypeData,
			tree.inserter,
			noNodeIndex,
			value,
		)
		tree.valueStore.release(value)
		if err != nil {
			return nil, fmt.Errorf("loading network %s: %w", prefix, err)
		}
	}
	// The audit only balances once the decoder's offset cache has released
	// its references. close is idempotent, so the deferred call is a no-op.
	decoder.close()
	if err := tree.maybeAuditValueStore(); err != nil {
		return nil, err
	}
	return tree, nil
}

func (t *Tree) normalizeLoadPrefix(prefix netip.Prefix) (netip.Prefix, error) {
	// Database readers should already return valid, masked prefixes. Only
	// normalize mapped IPv4 prefixes so loaded data follows Insert semantics.
	if !prefix.IsValid() {
		return netip.Prefix{}, errors.New("loaded prefix is invalid")
	}
	if !prefix.Addr().Is4In6() {
		return prefix, nil
	}

	normalized, err := t.normalizeInsertPrefix(prefix)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("normalizing loaded network %s: %w", prefix, err)
	}
	return normalized, nil
}

// Insert inserts a data value into the tree using the Tree's inserter function
// (defaults to inserter.Replace).
//
// You must never modify the value after insertion. Values may be shared with
// other records, and the tree caches values by object identity: if you mutate
// an inserted object in place, a later insert of that object can reuse the
// data from before the mutation.
//
// This is not safe to call from multiple threads.
func (t *Tree) Insert(prefix netip.Prefix, value mmdbtype.DataType) error {
	return t.insert(prefix, recordTypeData, t.inserter, false, noNodeIndex, value)
}

// InsertFunc will insert the output of the function passed to it. The arguments
// passed to the function are the existing value in the record and the new value
// passed to InsertFunc. The inserter function should return the
// mmdbtype.DataType to be inserted. In all cases, a nil value means an empty
// record.
//
// You must never modify arguments passed to the function as values may be
// shared with other records. If you want a copy of the mmdbtype.DataType to
// modify, call the Copy method on it, which will make a deep copy. This isn't
// done automatically before calling the function as not all functions will
// require the record to be copied and there is a non-trivial performance
// impact.
//
// The function is called separately for every covered record. Any
// non-nil value it returns becomes tree-owned and must not be modified after the
// function returns. A nil insertFunc returns an error.
//
// This is not safe to call from multiple threads.
func (t *Tree) InsertFunc(
	prefix netip.Prefix,
	value mmdbtype.DataType,
	insertFunc inserter.Func,
) error {
	if insertFunc == nil {
		return errNilInserterFunc
	}
	return t.insert(prefix, recordTypeData, insertFunc, false, noNodeIndex, value)
}

// InsertPureFunc is like InsertFunc, but the function's result and error must
// depend only on its arguments. It must not depend on invocation count, order,
// or external mutable state. Repeated argument pairs may be memoized during the
// insert, and a non-nil result may be shared by multiple records. A nil
// insertFunc returns an error.
//
// This is not safe to call from multiple threads.
func (t *Tree) InsertPureFunc(
	prefix netip.Prefix,
	value mmdbtype.DataType,
	insertFunc inserter.Func,
) error {
	if insertFunc == nil {
		return errNilInserterFunc
	}
	return t.insert(prefix, recordTypeData, insertFunc, true, noNodeIndex, value)
}

func (t *Tree) insert(
	prefix netip.Prefix,
	recordType recordType,
	insertFunc inserter.Func,
	inserterPure bool,
	node nodeIndex,
	value mmdbtype.DataType,
) error {
	var err error
	if recordType == recordTypeData {
		prefix, err = t.normalizeInsertPrefix(prefix)
		if err != nil {
			return err
		}
	} else {
		if !prefix.IsValid() {
			return errors.New("prefix is invalid")
		}
		prefix = prefix.Masked()
		if err := t.checkInsertPrefixFamily(prefix); err != nil {
			return err
		}
	}
	iRec, err := t.newInsertRecord(recordType, insertFunc, inserterPure, node, value)
	if err != nil {
		return err
	}
	err = t.insertPrepared(prefix, iRec)
	if err == nil {
		// Register the identity only once the tree references the value. A
		// registration that survived a failed insert would serve stale data
		// if the caller mutated and retried the same object.
		t.valueStore.rememberCallerIdentity(iRec.callerValue, iRec.value)
	}
	// The audit only balances once the memo and value references are gone, so
	// release explicitly rather than by defer.
	iRec.releaseResolved()
	if err != nil {
		return err
	}
	return t.maybeAuditValueStore()
}

// insertNormalizedRef inserts an already interned value. It borrows the
// caller's reference, taking one of its own for the insert.
func (t *Tree) insertNormalizedRef(
	prefix netip.Prefix,
	recordType recordType,
	insertFunc inserter.Func,
	node nodeIndex,
	value valueRef,
) error {
	if t.treeDepth == 32 && !prefix.Addr().Is4() {
		return errors.New("IPv6 prefixes cannot be inserted into an IPv4 tree")
	}
	t.valueStore.retain(value)
	iRec := t.newInsertRecordRef(recordType, insertFunc, false, node, value)
	defer iRec.releaseResolved()
	return t.insertPrepared(prefix, iRec)
}

func (t *Tree) insertPrepared(
	prefix netip.Prefix,
	iRec *insertRecord,
) error {
	// Any insert can change the reachable node graph, so cached finalization
	// state must be rebuilt before the next write.
	t.nodeCount = 0
	t.nodeNumbers = nil

	ip, prefixLen := t.prefixInsertIP(prefix)
	iRec.ip = ip
	iRec.prefixLen = prefixLen
	return iRec.insertNode(t.root, 0)
}

func (t *Tree) newInsertRecord(
	recordType recordType,
	insertFunc inserter.Func,
	inserterPure bool,
	node nodeIndex,
	value mmdbtype.DataType,
) (*insertRecord, error) {
	var ref valueRef
	if recordType == recordTypeData && value != nil {
		var err error
		ref, err = t.valueStore.intern(value)
		if err != nil {
			return nil, err
		}
	}
	iRec := t.newInsertRecordRef(recordType, insertFunc, inserterPure, node, ref)
	iRec.callerValue = value
	return iRec, nil
}

// newInsertRecordRef builds an insertRecord that owns the given reference,
// which releaseResolved releases.
func (t *Tree) newInsertRecordRef(
	recordType recordType,
	insertFunc inserter.Func,
	inserterPure bool,
	node nodeIndex,
	ref valueRef,
) *insertRecord {
	iRec := &insertRecord{
		recordType:   recordType,
		inserter:     insertFunc,
		inserterPure: inserterPure,
		insertedNode: node,
		tree:         t,
		value:        ref,

		store: t.valueStore,
	}
	if insertFunc != nil {
		iRec.valueView = t.valueStore.materialize(ref)
	}
	return iRec
}

func (t *Tree) normalizeInsertPrefix(prefix netip.Prefix) (netip.Prefix, error) {
	if !prefix.IsValid() {
		return netip.Prefix{}, errors.New("prefix is invalid")
	}

	addr := prefix.Addr()
	if addr.Is4() {
		return prefix.Masked(), nil
	}
	if addr.Is4In6() {
		if prefix.Bits() < 96 {
			return netip.Prefix{}, errors.New(
				"IPv4-mapped prefixes shorter than /96 cannot be inserted",
			)
		}
		return netip.PrefixFrom(addr.Unmap(), prefix.Bits()-96).Masked(), nil
	}
	if err := t.checkInsertPrefixFamily(prefix); err != nil {
		return netip.Prefix{}, err
	}
	return prefix.Masked(), nil
}

func (t *Tree) checkInsertPrefixFamily(prefix netip.Prefix) error {
	if t.treeDepth == 32 && !prefix.Addr().Is4() {
		return errors.New("IPv6 prefixes cannot be inserted into an IPv4 tree")
	}
	return nil
}

func (t *Tree) prefixInsertIP(prefix netip.Prefix) ([16]byte, int) {
	prefixLen := prefix.Bits()
	return t.addrInsertIP(prefix.Addr(), prefixLen)
}

func (t *Tree) addrInsertIP(addr netip.Addr, prefixLen int) ([16]byte, int) {
	var ip [16]byte

	if addr.Is4() {
		ip4 := addr.As4()
		if t.treeDepth == 128 {
			copy(ip[12:], ip4[:])
			return ip, prefixLen + 96
		}

		copy(ip[:4], ip4[:])
		return ip, prefixLen
	}

	ip16 := addr.As16()
	copy(ip[:], ip16[:])
	return ip, prefixLen
}

func (t *Tree) lookupIP(addr netip.Addr) ([16]byte, bool) {
	if !addr.IsValid() {
		return [16]byte{}, false
	}

	if t.treeDepth == 32 {
		addr = addr.Unmap()
		if !addr.Is4() {
			return [16]byte{}, false
		}
		ip, _ := t.addrInsertIP(addr, addr.BitLen())
		return ip, true
	}

	addr = addr.Unmap()
	ip, _ := t.addrInsertIP(addr, addr.BitLen())
	return ip, true
}

func (t *Tree) getPrefixForAddr(addr netip.Addr, prefixLen int) netip.Prefix {
	if !addr.IsValid() {
		return netip.Prefix{}
	}

	if t.treeDepth == 32 {
		addr = addr.Unmap()
		if !addr.Is4() {
			return netip.Prefix{}
		}
		prefix, err := addr.Prefix(prefixLen)
		if err != nil {
			return netip.Prefix{}
		}
		return prefix
	}

	if unmapped := addr.Unmap(); unmapped.Is4() {
		if prefixLen >= 96 {
			prefixLen -= 96
			addr = unmapped
		} else {
			ip, _ := t.addrInsertIP(unmapped, unmapped.BitLen())
			addr = netip.AddrFrom16(ip)
		}
	}

	prefix, err := addr.Prefix(prefixLen)
	if err != nil {
		return netip.Prefix{}
	}
	return prefix
}

// InsertRange is the same as Insert, except it will insert all subnets within
// the range of IPs specified by `[start,end]`.
func (t *Tree) InsertRange(
	start netip.Addr,
	end netip.Addr,
	value mmdbtype.DataType,
) error {
	return t.insertRange(start, end, recordTypeData, t.inserter, false, noNodeIndex, value)
}

// InsertRangeFunc is the same as InsertFunc, except it will insert all subnets
// within the range of IPs specified by `[start,end]`. A nil insertFunc returns
// an error.
func (t *Tree) InsertRangeFunc(
	start netip.Addr,
	end netip.Addr,
	value mmdbtype.DataType,
	insertFunc inserter.Func,
) error {
	if insertFunc == nil {
		return errNilInserterFunc
	}
	return t.insertRange(start, end, recordTypeData, insertFunc, false, noNodeIndex, value)
}

// InsertRangePureFunc is like InsertPureFunc, except it inserts all subnets
// within the range of IPs specified by `[start,end]`. Repeated argument pairs
// may be memoized across the entire range, not just within one of its subnets.
// A nil insertFunc returns an error.
func (t *Tree) InsertRangePureFunc(
	start netip.Addr,
	end netip.Addr,
	value mmdbtype.DataType,
	insertFunc inserter.Func,
) error {
	if insertFunc == nil {
		return errNilInserterFunc
	}
	return t.insertRange(start, end, recordTypeData, insertFunc, true, noNodeIndex, value)
}

func (t *Tree) insertRange(
	start netip.Addr,
	end netip.Addr,
	recordType recordType,
	insertFunc inserter.Func,
	inserterPure bool,
	node nodeIndex,
	value mmdbtype.DataType,
) error {
	if !start.IsValid() {
		return errors.New("start IP is invalid")
	}
	if !end.IsValid() {
		return errors.New("end IP is invalid")
	}
	start = start.Unmap()
	end = end.Unmap()
	if t.treeDepth == 32 && (!start.Is4() || !end.Is4()) {
		return errors.New("IPv6 ranges cannot be inserted into an IPv4 tree")
	}

	r := netipx.IPRangeFrom(start, end)
	if !r.IsValid() {
		return errors.New("start & end IPs did not give valid range")
	}
	iRec, err := t.newInsertRecord(recordType, insertFunc, inserterPure, node, value)
	if err != nil {
		return err
	}
	subnets := r.Prefixes()
	for _, subnet := range subnets {
		err = t.insertPrepared(subnet, iRec)
		if err != nil {
			break
		}
	}
	if err == nil {
		// Register the identity only once the tree references the value. A
		// registration that survived a failed insert would serve stale data
		// if the caller mutated and retried the same object.
		t.valueStore.rememberCallerIdentity(iRec.callerValue, iRec.value)
	}
	// The audit only balances once the memo and value references are gone, so
	// release explicitly rather than by defer.
	iRec.releaseResolved()
	if err != nil {
		return err
	}
	return t.maybeAuditValueStore()
}

func (t *Tree) insertStringNetwork(
	network string,
	recordType recordType,
	insertFunc inserter.Func,
	node nodeIndex,
) error {
	prefix, err := netip.ParsePrefix(network)
	if err != nil {
		return fmt.Errorf("parsing network (%s): %w", network, err)
	}
	return t.insert(prefix, recordType, insertFunc, false, node, nil)
}

var ipv4AliasNetworks = []string{
	"::ffff:0:0/96",
	"2001::/32",
	"2002::/16",
}

func (t *Tree) insertIPv4Aliases() error {
	ipv4Root, err := netip.ParsePrefix("::/96")
	if err != nil {
		return fmt.Errorf("parsing IPv4 root: %w", err)
	}

	ipv4RootNode := t.newNode([2]record{})

	// Make ::/96, the IPv4 root, a fixed node.
	err = t.insert(ipv4Root, recordTypeFixedNode, nil, false, ipv4RootNode, nil)
	if err != nil {
		return err
	}

	for _, network := range ipv4AliasNetworks {
		err := t.insertStringNetwork(network, recordTypeAlias, nil, ipv4RootNode)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) insertReservedNetworks() error {
	// the reserved networks are in reserved.go
	networks := reservedNetworksIPv4
	if t.ipVersion == 6 {
		networks = append(networks, reservedNetworksIPv6...)
	}

	for _, network := range networks {
		err := t.insertStringNetwork(network, recordTypeReserved, nil, noNodeIndex)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get the value for the given IP address from the tree. If the nil interface
// is returned, that means the tree does not have a value for the IP. If ip is
// invalid or cannot be looked up in this tree's IP version, the returned prefix
// is the zero value. Returned values are shared, read-only views that are equal
// to, but not necessarily the same objects as, the inserted values. Call Copy
// before modifying one.
//
// Get is not safe to call concurrently with any other Tree method, including
// other Get calls, because a lookup materializes its view lazily.
func (t *Tree) Get(ip netip.Addr) (netip.Prefix, mmdbtype.DataType) {
	lookupIP, ok := t.lookupIP(ip)
	if !ok {
		return netip.Prefix{}, nil
	}
	prefixLen, r := t.getNode(t.root, lookupIP, 0)

	var value mmdbtype.DataType
	if r.recordType == recordTypeData {
		value = t.valueStore.materialize(r.value)
	}

	return t.getPrefixForAddr(ip, prefixLen), value
}

// finalize prepares the tree for writing. It is not threadsafe.
func (t *Tree) finalize() {
	t.expandPaths(t.root, 0)
	t.nodeNumbers = make([]int, t.nodeCountAllocated)
	t.nodeCount = t.finalizeNode(t.root, 0)
}

// WriteTo writes the tree to the provided Writer.
func (t *Tree) WriteTo(w io.Writer) (int64, error) {
	if t.nodeCount == 0 {
		t.finalize()
	}

	buf := bufio.NewWriter(w)
	//nolint:errcheck // We check the error on flush the only place that matters.
	defer buf.Flush()

	// We create this here so that we don't have to allocate millions of these. This
	// may no longer make sense now that we are using a bufio.Writer anyway, which has
	// WriteByte, but we should probably do some testing.
	recordBuf := make([]byte, 2*t.recordSize/8)

	usePointers := true
	dataWriter := newDataWriter(t.valueStore, usePointers)

	nodeCount, numBytes, err := t.writeNode(buf, t.root, dataWriter, recordBuf)
	if err != nil {
		return numBytes, err
	}
	if nodeCount != t.nodeCount {
		// This should only happen if there is a programming bug
		// in this library.
		return numBytes, fmt.Errorf(
			"number of nodes written (%d) doesn't match number expected (%d)",
			nodeCount,
			t.nodeCount,
		)
	}

	nb, err := buf.Write(dataSectionSeparator)
	numBytes += int64(nb)
	if err != nil {
		return numBytes, fmt.Errorf("writing data section separator: %w", err)
	}

	nb64, err := dataWriter.WriteTo(buf)
	numBytes += nb64
	if err != nil {
		return numBytes, fmt.Errorf("writing data to buffer: %w", err)
	}

	nb, err = buf.Write(metadataStartMarker)
	numBytes += int64(nb)
	if err != nil {
		return numBytes, fmt.Errorf("writing metadata start marker: %w", err)
	}

	// The metadata gets its own store, so WriteTo does not mutate the tree's
	// store and the metadata writer's offset table stays metadata-sized.
	metadataWriter := newDataWriter(newValueStore(), !t.disableMetadataPointers)
	_, err = t.writeMetadata(metadataWriter)
	if err != nil {
		return numBytes, fmt.Errorf("writing metadata: %w", err)
	}

	nb64, err = metadataWriter.WriteTo(buf)
	numBytes += nb64
	if err != nil {
		return numBytes, fmt.Errorf("writing metadata to buffer: %w", err)
	}

	err = buf.Flush()
	if err != nil {
		return numBytes, fmt.Errorf("flushing buffer to writer: %w", err)
	}

	return numBytes, nil
}

func (t *Tree) writeNode(
	w io.Writer,
	nodeIndex nodeIndex,
	dataWriter *dataWriter,
	recordBuf []byte,
) (int, int64, error) {
	n := t.nodeAt(nodeIndex)
	err := t.copyNode(recordBuf, n, dataWriter)
	if err != nil {
		return 0, 0, err
	}

	numBytes := int64(0)
	nb, err := w.Write(recordBuf)
	numBytes += int64(nb)
	nodesWritten := 1
	if err != nil {
		return nodesWritten, numBytes, fmt.Errorf("writing node: %w", err)
	}

	for i := range 2 {
		child := &n.children[i]
		if child.recordType != recordTypeNode && child.recordType != recordTypeFixedNode {
			continue
		}
		addedNodes, addedBytes, err := t.writeNode(
			w,
			child.nodeIndex,
			dataWriter,
			recordBuf,
		)
		nodesWritten += addedNodes
		numBytes += addedBytes
		if err != nil {
			return nodesWritten, numBytes, err
		}
	}

	return nodesWritten, numBytes, nil
}

func (t *Tree) recordValue(
	r *record,
	dataWriter *dataWriter,
) (int, error) {
	switch r.recordType {
	case recordTypeData:
		offset, err := dataWriter.maybeWrite(r.value)
		return t.nodeCount + len(dataSectionSeparator) + offset, err
	case recordTypeEmpty, recordTypeReserved:
		return t.nodeCount, nil
	case recordTypePath:
		return 0, errors.New("compressed path record cannot be written before finalization")
	default:
		return t.nodeNumbers[r.nodeIndex], nil
	}
}

func (t *Tree) copyNode(buf []byte, n *node, dataWriter *dataWriter) error {
	left, err := t.recordValue(&n.children[0], dataWriter)
	if err != nil {
		return err
	}
	right, err := t.recordValue(&n.children[1], dataWriter)
	if err != nil {
		return err
	}

	maxRecord := int64(1) << t.recordSize
	if int64(left) >= maxRecord || int64(right) >= maxRecord {
		return fmt.Errorf(
			"exceeded record capacity by attempting to write (%d, %d) to node with %d bit record size; "+
				"try increasing RecordSize or reducing the size of the database",
			left,
			right,
			t.recordSize,
		)
	}

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
		buf[3] = byte(((((left >> 24) & 0x0F) << 4) | (right >> 24 & 0x0F)) & 0xFF)
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
		return fmt.Errorf("unsupported record size of %d", t.recordSize)
	}
	return nil
}

func (t *Tree) writeMetadata(dw *dataWriter) (int64, error) {
	description := make(mmdbtype.Map, len(t.description))
	for k, v := range t.description {
		description[mmdbtype.String(k)] = mmdbtype.String(v)
	}

	languages := make(mmdbtype.Slice, 0, len(t.languages))
	for _, v := range t.languages {
		languages = append(languages, mmdbtype.String(v))
	}
	if int64(t.nodeCount) > int64(math.MaxUint32) {
		return 0, fmt.Errorf("node count of %d exceeds the maximum allowed value", t.nodeCount)
	}
	metadata := mmdbtype.Map{
		"binary_format_major_version": mmdbtype.Uint16(2),
		"binary_format_minor_version": mmdbtype.Uint16(0),

		// Although it might make sense to change the type on this, there is no use
		// case where someone would reasonably pass a negative build epoch.
		//nolint:gosec // buildEpoch is validated to be non-negative
		"build_epoch":   mmdbtype.Uint64(t.buildEpoch),
		"database_type": mmdbtype.String(t.databaseType),
		"description":   description,
		//nolint:gosec // ipVersion is always 4 or 6
		"ip_version": mmdbtype.Uint16(t.ipVersion),
		"languages":  languages,
		//nolint:gosec // nodeCount is validated above
		"node_count": mmdbtype.Uint32(t.nodeCount),
		//nolint:gosec // recordSize is always 24, 28, or 32
		"record_size": mmdbtype.Uint16(t.recordSize),
	}
	ref, err := dw.store.intern(metadata)
	if err != nil {
		return 0, err
	}
	defer dw.store.release(ref)
	start := dw.Len()
	_, err = dw.maybeWrite(ref)
	return int64(dw.Len() - start), err
}
