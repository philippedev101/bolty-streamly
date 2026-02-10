{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Test.Sandwich

import           Control.Monad.IO.Class            (liftIO)
import           Data.Default                      (def)
import           Data.Function                     ((&))
import qualified Data.HashMap.Lazy                 as H
import qualified Data.Text                         as T
import qualified Data.Vector                       as V
import qualified Validation                        as Val
import           System.Environment                (lookupEnv)

import           Database.Bolty
import           Data.PackStream.Ps                (Ps(..))
import           Data.PackStream.Integer           (fromPSInteger)
import           Database.Bolty.Streamly           (queryStream, queryStreamP)
import qualified Streamly.Data.Stream              as Stream
import qualified Streamly.Data.Fold                as Fold


-- | Neo4j test config (same as bolty integration tests)
testConfig :: IO Config
testConfig = do
  h <- maybe "127.0.0.1" T.pack <$> lookupEnv "NEO4J_HOST"
  p <- maybe 7687 read <$> lookupEnv "NEO4J_PORT"
  pure def
    { host    = h
    , port    = p
    , scheme  = Basic "neo4j" "testpassword"
    , use_tls = False
    }


getConfig :: IO ValidatedConfig
getConfig = do
  cfg <- testConfig
  case validateConfig cfg of
    Val.Failure errs -> fail $ "Config invalid: " <> show errs
    Val.Success vc   -> pure vc


withPipe :: (Pipe -> IO a) -> IO a
withPipe f = do
  cfg <- getConfig
  pipe <- connect cfg
  result <- f pipe
  close pipe
  pure result


streamingTests :: TopSpec
streamingTests = describe "Streaming queries" $ do

  it "queryStream returns records one by one" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 5) AS n RETURN n"
      Stream.fold Fold.toList s
    length result `shouldBe` 5

  it "queryStream returns correct values" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 3) AS n RETURN n"
      Stream.fold Fold.toList s
    let values = map (\r -> asInt (V.head r)) result
    values `shouldBe` [Just 1, Just 2, Just 3]

  it "queryStream handles empty result set" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND [] AS n RETURN n"
      Stream.fold Fold.toList s
    length result `shouldBe` 0

  it "queryStream works with single row" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "RETURN 42 AS answer"
      Stream.fold Fold.toList s
    length result `shouldBe` 1
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 42
      Nothing -> expectationFailure "Expected 42"

  it "queryStreamP passes parameters" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStreamP "RETURN $x AS val" (H.singleton "x" (PsInteger 99))
      Stream.fold Fold.toList s
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 99
      Nothing -> expectationFailure "Expected 99"

  it "queryStreamP with list parameter" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStreamP "UNWIND $xs AS x RETURN x"
            (H.singleton "xs" (PsList $ V.fromList [PsInteger 10, PsInteger 20, PsInteger 30]))
      Stream.fold Fold.toList s
    length result `shouldBe` 3
    let values = map (\r -> asInt (V.head r)) result
    values `shouldBe` [Just 10, Just 20, Just 30]


streamFoldTests :: TopSpec
streamFoldTests = describe "Stream fold operations" $ do

  it "fold with Fold.length counts records" $ do
    count <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 10) AS n RETURN n"
      Stream.fold Fold.length s
    count `shouldBe` 10

  it "fold with Fold.sum sums values" $ do
    total <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 5) AS n RETURN n"
      Stream.fold (Fold.lmap (\r -> maybe 0 id (asInt (V.head r) >>= fromPSInteger)) Fold.sum) s
    (total :: Int) `shouldBe` 15

  it "Stream.take limits records" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 100) AS n RETURN n"
      Stream.fold Fold.toList $ Stream.take 3 s
    length result `shouldBe` 3


streamTransactionTests :: TopSpec
streamTransactionTests = describe "Streaming in transactions" $ do

  it "queryStream inside withTransaction" $ do
    result <- liftIO $ withPipe $ \p -> run p $ do
      withTransaction $ do
        s <- queryStream "UNWIND range(1, 5) AS n RETURN n"
        Stream.fold Fold.toList s
    length result `shouldBe` 5

  it "multiple queryStream calls in a transaction" $ do
    (r1, r2) <- liftIO $ withPipe $ \p -> run p $ do
      withTransaction $ do
        s1 <- queryStream "UNWIND [1,2,3] AS n RETURN n"
        res1 <- Stream.fold Fold.toList s1
        s2 <- queryStream "UNWIND [4,5,6] AS n RETURN n"
        res2 <- Stream.fold Fold.toList s2
        pure (res1, res2)
    length r1 `shouldBe` 3
    length r2 `shouldBe` 3

  it "queryStream with write in transaction" $ do
    liftIO $ withPipe $ \p -> run p $ do
      withTransaction $ do
        query "CREATE (n:TestStreamTx {value: 1})"
        s <- queryStream "MATCH (n:TestStreamTx) RETURN n.value AS v"
        result <- Stream.fold Fold.toList s
        liftIO $ length result `shouldBe` 1
    -- Cleanup
    liftIO $ withPipe $ \p -> run p $ query "MATCH (n:TestStreamTx) DELETE n" >> pure ()


largeResultTests :: TopSpec
largeResultTests = describe "Large result streaming" $ do

  it "streams 1000 rows without buffering all" $ do
    count <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 1000) AS n RETURN n"
      Stream.fold Fold.length s
    count `shouldBe` 1000

  it "streams 10000 rows" $ do
    count <- liftIO $ withPipe $ \p -> run p $ do
      s <- queryStream "UNWIND range(1, 10000) AS n RETURN n"
      Stream.fold Fold.length s
    count `shouldBe` 10000


main :: IO ()
main = runSandwichWithCommandLineArgs defaultOptions $ do
  streamingTests
  streamFoldTests
  streamTransactionTests
  largeResultTests
