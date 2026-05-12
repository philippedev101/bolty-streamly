# Changelog for `bolty-streamly`

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the
[Haskell Package Versioning Policy](https://pvp.haskell.org/).

## 0.1.0.2

- Bump bolty dependency bound to `>= 0.2.0.0 && < 0.3` (was `>= 0.1.1.0 && < 0.2`).
  Required by bolty 0.2.0.0's breaking changes: `Begin.mode` changed from `Char` to
  the new `AccessMode` type, and `AccessMode(..)` moved from `Database.Bolty.Routing`
  to the new `Database.Bolty.AccessMode` module (still re-exported from `Database.Bolty`).
- Internal: `sessionAcquire` now threads the `AccessMode` value directly into `Begin`
  instead of converting to a `Char` first. No behavioural change. Public API unchanged.

## 0.1.0.0

- Initial release
- Streamly Stream interface for bolty queries
- Lazy record-by-record consumption of Neo4j query results
- Connection pool, routing pool, and session streaming with automatic resource management via `bracketIO`
- Streams are composable values — store, pass, and combine them freely
- Typed decoding variants (`As` suffix) with `RowDecoder`
