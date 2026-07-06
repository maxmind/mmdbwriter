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
  was removed, `Options.Inserter` now accepts `inserter.Func`, and
  `Tree.InsertFunc` and `Tree.InsertRangeFunc` now take the new value as an
  argument.
- Reduced allocations on the tree insert and serialization hot paths, lowering
  memory pressure and GC overhead during large builds.
- `Load` now caches decoded source records by data offset during loading. This
  speeds up databases with repeated records, but the cache is retained until
  `Load` completes and can increase peak memory for very large source databases.
- Reworked tree storage to use an append-only indexed arena. This reduces
  pointer overhead and keeps node references stable, but merged or abandoned
  nodes and materialized sparse paths are retained until the `Tree` is
  discarded. Workloads with heavy mutation churn may see higher peak memory than
  v1.

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
