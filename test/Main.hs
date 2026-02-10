{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Test.Sandwich

import           Database.Bolty.Streamly (Stream, queryStream, queryStreamP, pullStream)
import           Database.Bolty          (BoltActionT, Record)
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


main :: IO ()
main = runSandwichWithCommandLineArgs defaultOptions tests
