module Main where

import           Test.Sandwich

import           Database.Bolty.Streamly (Stream, queryStream, queryStreamP, pullStream,
                                         poolStream, poolStreamP,
                                         routingStream, routingStreamP,
                                         sessionReadStream, sessionReadStreamP,
                                         sessionWriteStream, sessionWriteStreamP,
                                         queryStreamAs, queryStreamPAs,
                                         poolStreamAs, poolStreamPAs,
                                         routingStreamAs, routingStreamPAs,
                                         sessionReadStreamAs, sessionReadStreamPAs,
                                         sessionWriteStreamAs, sessionWriteStreamPAs)
import           Database.Bolty          (BoltPool, Connection, Record, Session,
                                         AccessMode(..))
import           Data.Text              (Text)
import           Database.Bolty.Decode   (RowDecoder, column, int64)
import           Database.Bolty.Routing  (RoutingPool)
import           Data.Int                (Int64)
import qualified Data.HashMap.Lazy       as H
import           Data.PackStream.Ps      (Ps(..))


-- Type-level checks: these ensure the module exports compile with
-- the correct types. They are never called at runtime.

_checkQueryStream :: Connection -> IO (Stream IO Record)
_checkQueryStream conn = queryStream conn "RETURN 1"

_checkQueryStreamP :: Connection -> IO (Stream IO Record)
_checkQueryStreamP conn = queryStreamP conn "RETURN $x" (H.singleton "x" (PsInteger 42))

_checkPullStream :: Connection -> IO (Stream IO Record)
_checkPullStream conn = pullStream conn

_checkPoolStream :: BoltPool -> Stream IO Record
_checkPoolStream pool = poolStream pool "RETURN 1"

_checkPoolStreamP :: BoltPool -> Stream IO Record
_checkPoolStreamP pool =
  poolStreamP pool "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkRoutingStream :: RoutingPool -> Stream IO Record
_checkRoutingStream rp = routingStream rp ReadAccess "RETURN 1"

_checkRoutingStreamP :: RoutingPool -> Stream IO Record
_checkRoutingStreamP rp =
  routingStreamP rp ReadAccess "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkSessionReadStream :: Session -> Stream IO Record
_checkSessionReadStream s = sessionReadStream s "RETURN 1"

_checkSessionReadStreamP :: Session -> Stream IO Record
_checkSessionReadStreamP s =
  sessionReadStreamP s "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkSessionWriteStream :: Session -> Stream IO Record
_checkSessionWriteStream s = sessionWriteStream s "RETURN 1"

_checkSessionWriteStreamP :: Session -> Stream IO Record
_checkSessionWriteStreamP s =
  sessionWriteStreamP s "RETURN $x" (H.singleton "x" (PsInteger 1))

-- Decode variant type-level checks

_decoder :: RowDecoder Int64
_decoder = column 0 int64

_checkQueryStreamAs :: Connection -> IO (Stream IO Int64)
_checkQueryStreamAs conn = queryStreamAs _decoder conn "RETURN 1"

_checkQueryStreamPAs :: Connection -> IO (Stream IO Int64)
_checkQueryStreamPAs conn = queryStreamPAs _decoder conn "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkPoolStreamAs :: BoltPool -> Stream IO Int64
_checkPoolStreamAs pool = poolStreamAs _decoder pool "RETURN 1"

_checkPoolStreamPAs :: BoltPool -> Stream IO Int64
_checkPoolStreamPAs pool =
  poolStreamPAs _decoder pool "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkRoutingStreamAs :: RoutingPool -> Stream IO Int64
_checkRoutingStreamAs rp =
  routingStreamAs _decoder rp ReadAccess "RETURN 1"

_checkRoutingStreamPAs :: RoutingPool -> Stream IO Int64
_checkRoutingStreamPAs rp =
  routingStreamPAs _decoder rp ReadAccess "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkSessionReadStreamAs :: Session -> Stream IO Int64
_checkSessionReadStreamAs s = sessionReadStreamAs _decoder s "RETURN 1"

_checkSessionReadStreamPAs :: Session -> Stream IO Int64
_checkSessionReadStreamPAs s =
  sessionReadStreamPAs _decoder s "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkSessionWriteStreamAs :: Session -> Stream IO Int64
_checkSessionWriteStreamAs s = sessionWriteStreamAs _decoder s "RETURN 1"

_checkSessionWriteStreamPAs :: Session -> Stream IO Int64
_checkSessionWriteStreamPAs s =
  sessionWriteStreamPAs _decoder s "RETURN $x" (H.singleton "x" (PsInteger 1))


tests :: TopSpec
tests = describe "Database.Bolty.Streamly" $ do

  it "queryStream has correct type" $ do
    let _ = _checkQueryStream
    pure ()

  it "queryStreamP has correct type" $ do
    let _ = _checkQueryStreamP
    pure ()

  it "pullStream has correct type" $ do
    let _ = _checkPullStream
    pure ()

  it "Stream type is re-exported" $ do
    let _ = undefined :: Stream IO Int
    pure ()

  it "poolStream has correct type" $ do
    let _ = _checkPoolStream
    pure ()

  it "poolStreamP has correct type" $ do
    let _ = _checkPoolStreamP
    pure ()

  it "routingStream has correct type" $ do
    let _ = _checkRoutingStream
    pure ()

  it "routingStreamP has correct type" $ do
    let _ = _checkRoutingStreamP
    pure ()

  it "sessionReadStream has correct type" $ do
    let _ = _checkSessionReadStream
    pure ()

  it "sessionReadStreamP has correct type" $ do
    let _ = _checkSessionReadStreamP
    pure ()

  it "sessionWriteStream has correct type" $ do
    let _ = _checkSessionWriteStream
    pure ()

  it "sessionWriteStreamP has correct type" $ do
    let _ = _checkSessionWriteStreamP
    pure ()

  it "queryStreamAs has correct type" $ do
    let _ = _checkQueryStreamAs
    pure ()

  it "queryStreamPAs has correct type" $ do
    let _ = _checkQueryStreamPAs
    pure ()

  it "poolStreamAs has correct type" $ do
    let _ = _checkPoolStreamAs
    pure ()

  it "poolStreamPAs has correct type" $ do
    let _ = _checkPoolStreamPAs
    pure ()

  it "routingStreamAs has correct type" $ do
    let _ = _checkRoutingStreamAs
    pure ()

  it "routingStreamPAs has correct type" $ do
    let _ = _checkRoutingStreamPAs
    pure ()

  it "sessionReadStreamAs has correct type" $ do
    let _ = _checkSessionReadStreamAs
    pure ()

  it "sessionReadStreamPAs has correct type" $ do
    let _ = _checkSessionReadStreamPAs
    pure ()

  it "sessionWriteStreamAs has correct type" $ do
    let _ = _checkSessionWriteStreamAs
    pure ()

  it "sessionWriteStreamPAs has correct type" $ do
    let _ = _checkSessionWriteStreamPAs
    pure ()

  -- Verify pool/routing/session functions return Stream (not IO Stream)
  -- This is the key bracketIO design property: streams are pure values.

  it "poolStream returns Stream IO Record (not IO)" $ do
    let _ = poolStream :: BoltPool -> Text -> Stream IO Record
    pure ()

  it "routingStream returns Stream IO Record (not IO)" $ do
    let _ = routingStream :: RoutingPool -> AccessMode -> Text -> Stream IO Record
    pure ()

  it "sessionReadStream returns Stream IO Record (not IO)" $ do
    let _ = sessionReadStream :: Session -> Text -> Stream IO Record
    pure ()

  it "sessionWriteStream returns Stream IO Record (not IO)" $ do
    let _ = sessionWriteStream :: Session -> Text -> Stream IO Record
    pure ()

  it "poolStreamAs returns Stream IO a (not IO)" $ do
    let _ = poolStreamAs :: RowDecoder Int64 -> BoltPool -> Text -> Stream IO Int64
    pure ()

  it "routingStreamAs returns Stream IO a (not IO)" $ do
    let _ = routingStreamAs :: RowDecoder Int64 -> RoutingPool -> AccessMode -> Text -> Stream IO Int64
    pure ()

  it "sessionReadStreamAs returns Stream IO a (not IO)" $ do
    let _ = sessionReadStreamAs :: RowDecoder Int64 -> Session -> Text -> Stream IO Int64
    pure ()

  -- Verify bare-connection functions still return IO (Stream IO Record)
  -- since they perform RUN eagerly before streaming PULL.

  it "queryStream returns IO (Stream IO Record)" $ do
    let _ = queryStream :: Connection -> Text -> IO (Stream IO Record)
    pure ()

  it "queryStreamAs returns IO (Stream IO a)" $ do
    let _ = queryStreamAs :: RowDecoder Int64 -> Connection -> Text -> IO (Stream IO Int64)
    pure ()


main :: IO ()
main = runSandwichWithCommandLineArgs defaultOptions tests
