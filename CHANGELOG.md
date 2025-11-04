# CHANGELOG

## 1.2.0

* The `mmdbtype.Unmarshaler` now caches nested structures (Maps, Slices, and
  Uint128 values) in addition to top-level values. This improves performance
  when loading databases with shared nested data structures. Simple scalar types
  are intentionally not cached as they are cheap to decode and caching would
  waste memory and CPU cycles.
* The zero value of `mmdbtype.Unmarshaler` is now documented as safe to use
  for unmarshaling without caching enabled. Use `NewUnmarshaler()` when you
  want caching.

## 1.1.0 (2025-10-08)

* Removed unnecessary deep copies in inserter. GitHub #119.
* Converted to IPv4 in reserved network errors when inserting IPv4 into an
  IPv6 tree. GitHub #77.
* Added typed errors for errors inserting into aliased and reserved
  networks. GitHub #71.
* Added support for custom key generators. GitHub #70.
* Improved performance of the default key generator. GitHub #70.

## 1.0.0 (2023-09-27)

* First tagged release.
