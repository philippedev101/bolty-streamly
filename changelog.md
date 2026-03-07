# Changelog for `bolty-streamly`

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the
[Haskell Package Versioning Policy](https://pvp.haskell.org/).

## 0.1.0.0

- Initial release
- Streamly Stream interface for bolty queries
- Lazy record-by-record consumption of Neo4j query results
- Connection pool, routing pool, and session streaming with automatic resource management via `bracketIO`
- Streams are composable values — store, pass, and combine them freely
- Typed decoding variants (`As` suffix) with `RowDecoder`
