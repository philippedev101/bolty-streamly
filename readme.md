# bolty-streamly

Streamly streaming interface for the [bolty](https://github.com/philippedev101/bolty) Neo4j driver.

Wraps bolty's query functions with streamly's `Stream` type for lazy record-by-record consumption, avoiding loading entire result sets into memory.

## Usage

```haskell
import qualified Database.Bolty          as Bolt
import qualified Database.Bolty.Streamly as BoltS
import qualified Streamly.Data.Stream    as Stream

main :: IO ()
main = do
  conn <- Bolt.connect def
  stream <- BoltS.query conn "MATCH (n) RETURN n"
  Stream.mapM_ print stream
  Bolt.close conn
```

## License

Apache-2.0
