# bolty-streamly

Streaming interface for the [bolty](https://github.com/philippedev101/bolty) Neo4j driver, built on [streamly](https://hackage.haskell.org/package/streamly-core).

## Why streaming?

bolty's standard `query` functions buffer the entire result set into a `Vector` before returning. For large result sets (millions of rows, graph traversals, data exports), this can use a lot of memory.

bolty-streamly yields records **one at a time** as they arrive from the server, giving you constant-memory consumption regardless of result set size. Records are pulled from Neo4j in batches via the BOLT protocol's PULL mechanism, but exposed as a single `Stream IO` — you never have to think about batching.

## Quick start

```haskell
import qualified Database.Bolty          as Bolt
import qualified Database.Bolty.Streamly as BoltS
import qualified Streamly.Data.Stream    as Stream
import qualified Streamly.Data.Fold      as Fold
import           Data.Default            (def)

main :: IO ()
main = do
  let cfg = def{ Bolt.scheme = Bolt.Basic "neo4j" "password", Bolt.use_tls = False }
  case Bolt.validateConfig cfg of
    Failure _ -> error "bad config"
    Success vc -> do
      conn <- Bolt.connect vc
      s <- BoltS.queryStream conn "MATCH (n:Person) RETURN n.name AS name, n.age AS age"
      count <- Stream.fold Fold.length s
      putStrLn $ "Processed " <> show count <> " records"
      Bolt.close conn
```

## API overview

The module exposes four levels of streaming, each with variants for parameters (`P`) and typed decoding (`As`):

### Direct connection

Use when you manage the connection yourself:

```haskell
-- Raw records
queryStream   :: Connection -> Text -> IO (Stream IO Record)
queryStreamP  :: Connection -> Text -> HashMap Text Ps -> IO (Stream IO Record)

-- Decoded records (throws DecodeError on failure)
queryStreamAs  :: RowDecoder a -> Connection -> Text -> IO (Stream IO a)
queryStreamPAs :: RowDecoder a -> Connection -> Text -> HashMap Text Ps -> IO (Stream IO a)
```

### Connection pool

Acquires a connection, streams the query, and releases when the consumer returns. **The stream must be fully consumed within the callback** — the connection is returned to the pool when `consume` finishes:

```haskell
withPoolStream   :: BoltPool -> Text -> (Stream IO Record -> IO a) -> IO a
withPoolStreamAs :: RowDecoder a -> BoltPool -> Text -> (Stream IO a -> IO b) -> IO b
-- + P variants for parameters
```

Example:

```haskell
pool <- Bolt.createPool vc Bolt.defaultPoolConfig

withPoolStreamAs personDecoder pool "MATCH (p:Person) RETURN p.name, p.age" $ \stream ->
  Stream.mapM_ (\person -> putStrLn (show person)) stream

Bolt.destroyPool pool
```

### Routing pool (clusters)

Routes queries to the appropriate cluster member based on access mode:

```haskell
withRoutingStream   :: RoutingPool -> AccessMode -> Text -> (Stream IO Record -> IO a) -> IO a
withRoutingStreamAs :: RowDecoder a -> RoutingPool -> AccessMode -> Text -> (Stream IO a -> IO b) -> IO b
-- + P variants for parameters
```

Example:

```haskell
withRoutingStreamAs decoder routingPool ReadAccess "MATCH (n) RETURN n" $ \stream ->
  Stream.fold Fold.toList stream
```

### Session (causal consistency)

Runs streaming queries inside managed transactions with automatic bookmark tracking, retries on transient errors, and read/write routing:

```haskell
sessionReadStream   :: Session -> Text -> (Stream IO Record -> IO a) -> IO a
sessionWriteStream  :: Session -> Text -> (Stream IO Record -> IO a) -> IO a
sessionReadStreamAs :: RowDecoder a -> Session -> Text -> (Stream IO a -> IO b) -> IO b
-- + P and Write variants
```

Example:

```haskell
session <- Bolt.createSession pool Bolt.defaultSessionConfig

-- Write some data
sessionWriteStream session "CREATE (p:Person {name: 'Alice'})" $ \s ->
  Stream.fold Fold.drain s

-- Read it back (guaranteed to see Alice via bookmarks)
sessionReadStreamAs personDecoder session "MATCH (p:Person) RETURN p.name, p.age" $ \stream ->
  Stream.mapM_ print stream
```

## Low-level: pullStream

If you need to run a query with custom RUN parameters and then stream the PULL phase yourself:

```haskell
pullStream :: Connection -> IO (Stream IO Record)
```

This expects the connection to already be in `Streaming` or `TXstreaming` state (after a RUN has been acknowledged). It handles PULL batching and state transitions automatically.

## Important: stream lifetime

With the pool, routing, and session variants, the `Stream` is **only valid inside the callback**. The connection is released when the callback returns, so you cannot store the stream or consume it later:

```haskell
-- WRONG: stream escapes the callback
stream <- withPoolStream pool "MATCH (n) RETURN n" pure  -- connection released!
Stream.mapM_ print stream  -- BOOM: connection already returned to pool

-- RIGHT: consume inside the callback
withPoolStream pool "MATCH (n) RETURN n" $ \stream ->
  Stream.mapM_ print stream
```

With `queryStream` / `queryStreamP` on a bare connection, you manage the connection lifetime yourself, so the stream lives as long as the connection does.

## Naming convention

| Suffix | Meaning |
|---|---|
| *(none)* | No parameters, raw `Record` stream |
| `P` | With parameters (`HashMap Text Ps`) |
| `As` | Decoded via `RowDecoder a`, no parameters |
| `PAs` | Decoded via `RowDecoder a`, with parameters |

## Supported GHC versions

9.6.7, 9.8.4, 9.10.3, 9.12.3

## License

Apache-2.0
