# CHANGELOG

## 2.0.0

- Changed the module path to `github.com/maxmind/mmdbwriter/v2`.
- Changed `Tree.Insert`, `Tree.InsertFunc`, `Tree.InsertRange`,
  `Tree.InsertRangeFunc`, and `Tree.Get` to use `net/netip` types instead of
  `net.IP` and `net.IPNet`. This removes conversion overhead for callers that
  already use `netip.Prefix` values and for database loading through
  maxminddb-golang. Inserted prefixes are masked before insertion, invalid
  prefixes now return errors, IPv6 prefixes and ranges are rejected by IPv4
  trees, IPv4-mapped prefixes at `/96` or longer are treated as IPv4 prefixes,
  IPv4-mapped prefixes shorter than `/96` are rejected, and aliased or reserved
  network errors report masked `netip.Prefix` values.
- Changed the inserter API so inserters receive both the existing value and the
  new value directly. This removes per-insertion closure generation from merge
  insertion paths. `inserter.ReplaceWith`, `inserter.TopLevelMergeWith`, and
  `inserter.DeepMergeWith` are replaced by `inserter.Replace`,
  `inserter.TopLevelMerge`, and `inserter.DeepMerge`. `inserter.FuncGenerator`
  was removed, `Options.Inserter` now accepts an `inserter.Func`, and
  `Tree.InsertFunc` and `Tree.InsertRangeFunc` now take the new value and a
  function. These functions are evaluated separately for every covered record,
  as in v1. Added `Tree.InsertPureFunc` and `Tree.InsertRangePureFunc` for
  functions whose result and error depend only on their arguments. These methods
  may memoize repeated argument pairs within an insertion;
  `Tree.InsertRangePureFunc` shares the memo across the entire range. A nil
  function passed to `Tree.InsertFunc`, `Tree.InsertRangeFunc`,
  `Tree.InsertPureFunc`, or `Tree.InsertRangePureFunc` now returns an error;
  `Options.Inserter` may still be nil, which is equivalent to
  `inserter.Replace`. Non-nil results become tree-owned and must not be modified
  after the function returns.
- Reduced allocations on the tree insert and serialization hot paths, lowering
  memory pressure and GC overhead during large builds.
- Reworked value storage to intern every value node once in a content-addressed
  store: scalars, strings, and each nested map and slice. A seeded structural
  hash keys the store, and an exact comparison resolves each collision. Scalars
  and strings hold their final MMDB wire encoding in shared arenas, and
  containers hold canonical child references. The store otherwise releases
  inserted Go value graphs instead of retaining them, which substantially
  reduces peak memory for large builds with repeated or overlapping data. Two
  caches trade some retention for speed. A bounded cache keeps up to about one
  million recently inserted caller values, evicted least recently used, so
  repeated inserts of the same object are cheap. Values that an inserter or
  `Tree.Get` reads are materialized once and kept on their store nodes for later
  lookups and merges. Values returned by `Tree.Get`, and existing values passed
  to inserter functions, are shared, read-only views. They are equal to the
  inserted values but are not necessarily the same Go objects. The new value an
  inserter receives is the value passed to the insert call, or a shared view of
  the decoded record during `Load`. Treat both arguments as read-only. Call
  `Copy` before you modify such a value, and never modify a value after you
  insert it.
- `Load` now interns records into the value store directly from the database
  decoder, without building intermediate Go map and slice graphs. A cache of one
  stored reference per source data offset makes repeated records cheap. `Load`
  releases the cache before it returns. Networks that share a data record share
  one stored value, so custom inserters must copy values before modifying them.
  A non-nil `Options.Inserter` also materializes a view of each decoded record
  for its callback.
- A `Tree` is not safe for concurrent use. In v1, concurrent lookups on a tree
  that was not being modified were safe. In v2, lookups materialize shared views
  lazily, so the caller must synchronize even concurrent `Tree.Get` calls.
- Added `Options.RefcountAudit`, which makes the tree audit its reference counts
  after each successful insert and load. Setting the `MMDBWRITER_REFCOUNT_AUDIT`
  environment variable turns the audit on for every tree in the process. The
  audit is a debugging tool. It slows each insert to a full walk of the tree and
  the store.
- Inserting a raw `mmdbtype.Pointer` value now returns an error. The previous
  writer emitted it as a literal, dangling pointer that no reader could resolve.
- Inserting a negative or wider-than-128-bit `mmdbtype.Uint128` now returns an
  error. The wire encoding holds only the magnitude, so a negative value
  previously encoded as its absolute value and produced incorrect data.
- The two validations above apply to direct inserts and to inserter results. A
  custom inserter can receive an unsupported input value and must replace or
  discard it.
- Reworked tree storage to use an append-only indexed arena. This reduces
  pointer overhead and keeps node references stable, but merged or abandoned
  nodes and materialized sparse paths are retained until the `Tree` is
  discarded. Workloads with heavy mutation churn may see higher peak memory than
  v1.
- Removed `Options.KeyGenerator` and the `KeyGenerator` interface. There is no
  replacement. Record values are now indexed by a seeded structural content
  hash, and values are compared exactly before deduplication, so hash collisions
  cannot substitute a different value. A generator supplied for performance can
  simply be deleted, as the built-in hashing subsumes it. A generator that
  deliberately returned the same key for values you wanted collapsed has no
  equivalent: those values are now kept separate, which changes output rather
  than failing to compile.
- Changed `inserter.DeepMerge` to reuse existing maps and slices when a merge
  does not change their contents and retain unchanged nested containers,
  avoiding unnecessary cloning and reindexing. Its result must therefore be
  treated as immutable.
- `Load` now returns an error when a database's metadata declares an unsupported
  `ip_version` or `record_size` rather than using the value unchecked.
- `New` now returns an error for an unsupported `Options.RecordSize`. Such a
  value previously reached serialization, where a negative one panicked and any
  other unsupported one failed only after output had been written.
- `New` now returns an error for a negative `Options.BuildEpoch`. It was
  previously written to the metadata as a very large `build_epoch`, producing a
  database that readers accept but that carries a nonsense build time.
- `mmdbtype.Uint128.Equal` now returns false when either value is a nil
  `*Uint128`. Previously a nil argument caused a panic.
- `mmdbtype.Float32.Equal` and `mmdbtype.Float64.Equal` now compare the wire
  encoding rather than the Go value. `+0.0` and `-0.0` are no longer equal, and
  two NaNs with the same bit pattern now are. This makes equality agree with
  deduplication, which is already exact. In v1 an inserted signed zero was kept
  or discarded depending on whether an unrelated sibling key changed in the same
  insertion; it is now always kept.

## 1.2.0 (2026-01-14)

- The `mmdbtype.Unmarshaler` now caches nested structures, maps and slices, in
  addition to top-level values. This improves performance when loading databases
  with shared nested data structures.
- The zero value of `mmdbtype.Unmarshaler` is now documented as safe to use for
  unmarshaling without caching enabled. Use `NewUnmarshaler()` when you want
  caching.

## 1.1.0 (2025-10-08)

- Removed unnecessary deep copies in inserter. GitHub #119.
- Converted to IPv4 in reserved network errors when inserting IPv4 into an IPv6
  tree. GitHub #77.
- Added typed errors for errors inserting into aliased and reserved networks.
  GitHub #71.
- Added support for custom key generators. GitHub #70.
- Improved performance of the default key generator. GitHub #70.

## 1.0.0 (2023-09-27)

- First tagged release.
