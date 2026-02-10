{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Test.Sandwich

import           Database.Bolty.Streamly (Stream, queryStream, queryStreamP, pullStream,
                                         withPoolStream, withPoolStreamP,
                                         withRoutingStream, withRoutingStreamP,
                                         sessionReadStream, sessionReadStreamP,
                                         sessionWriteStream, sessionWriteStreamP)
import           Database.Bolty          (BoltActionT, BoltPool, Record, Session,
                                         AccessMode(..))
import           Database.Bolty.Routing  (RoutingPool)
import qualified Data.HashMap.Lazy       as H
import           Data.PackStream.Ps      (Ps(..))


-- Type-level checks: these ensure the module exports compile with
-- the correct types. They are never called at runtime.

_checkQueryStream :: BoltActionT IO (Stream (BoltActionT IO) Record)
_checkQueryStream = queryStream "RETURN 1"

_checkQueryStreamP :: BoltActionT IO (Stream (BoltActionT IO) Record)
_checkQueryStreamP = queryStreamP "RETURN $x" (H.singleton "x" (PsInteger 42))

_checkPullStream :: Stream (BoltActionT IO) Record
_checkPullStream = pullStream

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


main :: IO ()
main = runSandwichWithCommandLineArgs defaultOptions tests
