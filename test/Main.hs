{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Test.Sandwich

import           Database.Bolty.Streamly (Stream, queryStream, queryStreamP, pullStream,
                                         withPoolStream, withPoolStreamP,
                                         withRoutingStream, withRoutingStreamP,
                                         sessionReadStream, sessionReadStreamP,
                                         sessionWriteStream, sessionWriteStreamP,
                                         queryStreamAs, queryStreamPAs,
                                         withPoolStreamAs, withPoolStreamPAs,
                                         withRoutingStreamAs, withRoutingStreamPAs,
                                         sessionReadStreamAs, sessionReadStreamPAs,
                                         sessionWriteStreamAs, sessionWriteStreamPAs)
import           Database.Bolty          (BoltPool, Connection, Record, Session,
                                         AccessMode(..))
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

_checkWithPoolStream :: BoltPool -> IO Int
_checkWithPoolStream pool = withPoolStream pool "RETURN 1" $ \_ -> pure 1

_checkWithPoolStreamP :: BoltPool -> IO Int
_checkWithPoolStreamP pool =
  withPoolStreamP pool "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

_checkWithRoutingStream :: RoutingPool -> IO Int
_checkWithRoutingStream rp =
  withRoutingStream rp ReadAccess "RETURN 1" $ \_ -> pure 1

_checkWithRoutingStreamP :: RoutingPool -> IO Int
_checkWithRoutingStreamP rp =
  withRoutingStreamP rp ReadAccess "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

_checkSessionReadStream :: Session -> IO Int
_checkSessionReadStream s = sessionReadStream s "RETURN 1" $ \_ -> pure 1

_checkSessionReadStreamP :: Session -> IO Int
_checkSessionReadStreamP s =
  sessionReadStreamP s "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

_checkSessionWriteStream :: Session -> IO Int
_checkSessionWriteStream s = sessionWriteStream s "RETURN 1" $ \_ -> pure 1

_checkSessionWriteStreamP :: Session -> IO Int
_checkSessionWriteStreamP s =
  sessionWriteStreamP s "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

-- Decode variant type-level checks

_decoder :: RowDecoder Int64
_decoder = column 0 int64

_checkQueryStreamAs :: Connection -> IO (Stream IO Int64)
_checkQueryStreamAs conn = queryStreamAs _decoder conn "RETURN 1"

_checkQueryStreamPAs :: Connection -> IO (Stream IO Int64)
_checkQueryStreamPAs conn = queryStreamPAs _decoder conn "RETURN $x" (H.singleton "x" (PsInteger 1))

_checkWithPoolStreamAs :: BoltPool -> IO Int
_checkWithPoolStreamAs pool = withPoolStreamAs _decoder pool "RETURN 1" $ \_ -> pure 1

_checkWithPoolStreamPAs :: BoltPool -> IO Int
_checkWithPoolStreamPAs pool =
  withPoolStreamPAs _decoder pool "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

_checkWithRoutingStreamAs :: RoutingPool -> IO Int
_checkWithRoutingStreamAs rp =
  withRoutingStreamAs _decoder rp ReadAccess "RETURN 1" $ \_ -> pure 1

_checkWithRoutingStreamPAs :: RoutingPool -> IO Int
_checkWithRoutingStreamPAs rp =
  withRoutingStreamPAs _decoder rp ReadAccess "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

_checkSessionReadStreamAs :: Session -> IO Int
_checkSessionReadStreamAs s = sessionReadStreamAs _decoder s "RETURN 1" $ \_ -> pure 1

_checkSessionReadStreamPAs :: Session -> IO Int
_checkSessionReadStreamPAs s =
  sessionReadStreamPAs _decoder s "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1

_checkSessionWriteStreamAs :: Session -> IO Int
_checkSessionWriteStreamAs s = sessionWriteStreamAs _decoder s "RETURN 1" $ \_ -> pure 1

_checkSessionWriteStreamPAs :: Session -> IO Int
_checkSessionWriteStreamPAs s =
  sessionWriteStreamPAs _decoder s "RETURN $x" (H.singleton "x" (PsInteger 1)) $ \_ -> pure 1


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

  it "withPoolStream has correct type" $ do
    let _ = _checkWithPoolStream
    pure ()

  it "withPoolStreamP has correct type" $ do
    let _ = _checkWithPoolStreamP
    pure ()

  it "withRoutingStream has correct type" $ do
    let _ = _checkWithRoutingStream
    pure ()

  it "withRoutingStreamP has correct type" $ do
    let _ = _checkWithRoutingStreamP
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

  it "withPoolStreamAs has correct type" $ do
    let _ = _checkWithPoolStreamAs
    pure ()

  it "withPoolStreamPAs has correct type" $ do
    let _ = _checkWithPoolStreamPAs
    pure ()

  it "withRoutingStreamAs has correct type" $ do
    let _ = _checkWithRoutingStreamAs
    pure ()

  it "withRoutingStreamPAs has correct type" $ do
    let _ = _checkWithRoutingStreamPAs
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


main :: IO ()
main = runSandwichWithCommandLineArgs defaultOptions tests
