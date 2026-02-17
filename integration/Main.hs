{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Test.Sandwich

import           Control.Monad.IO.Class            (liftIO)
import           Data.Default                      (def)
import qualified Data.HashMap.Lazy                 as H
import qualified Data.Text                         as T
import qualified Data.Vector                       as V
import qualified Validation                        as Val
import           System.Environment                (lookupEnv)

import           Database.Bolty
import           Database.Bolty.Connection         (queryIO)
import           Data.PackStream.Ps                (Ps(..))
import           Data.PackStream.Integer           (fromPSInteger)
import           Data.Int                          (Int64)
import qualified Control.Exception                 as E
import           Database.Bolty.Decode             (DecodeError(..), RowDecoder, column,
                                                    field, int64, text)
import           Database.Bolty.Streamly           (queryStream, queryStreamP,
                                                    queryStreamAs, queryStreamPAs,
                                                    withPoolStream, withPoolStreamP,
                                                    withPoolStreamAs,
                                                    withRoutingStream, withRoutingStreamP,
                                                    sessionReadStream, sessionReadStreamP,
                                                    sessionWriteStream, sessionWriteStreamP,
                                                    sessionReadStreamAs)
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


getRoutingConfig :: IO ValidatedConfig
getRoutingConfig = do
  cfg <- testConfig
  let routingCfg = cfg { routing = Routing }
  case validateConfig routingCfg of
    Val.Failure errs -> fail $ "Config invalid: " <> show errs
    Val.Success vc   -> pure vc


withConn :: (Connection -> IO a) -> IO a
withConn f = do
  cfg <- getConfig
  conn <- connect cfg
  result <- f conn
  close conn
  pure result


streamingTests :: TopSpec
streamingTests = describe "Streaming queries" $ do

  it "queryStream returns records one by one" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 5) AS n RETURN n"
      Stream.fold Fold.toList s
    length result `shouldBe` 5

  it "queryStream returns correct values" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 3) AS n RETURN n"
      Stream.fold Fold.toList s
    let values = map (\r -> asInt (V.head r)) result
    values `shouldBe` [Just 1, Just 2, Just 3]

  it "queryStream handles empty result set" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND [] AS n RETURN n"
      Stream.fold Fold.toList s
    length result `shouldBe` 0

  it "queryStream works with single row" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStream p "RETURN 42 AS answer"
      Stream.fold Fold.toList s
    length result `shouldBe` 1
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 42
      Nothing -> expectationFailure "Expected 42"

  it "queryStreamP passes parameters" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStreamP p "RETURN $x AS val" (H.singleton "x" (PsInteger 99))
      Stream.fold Fold.toList s
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 99
      Nothing -> expectationFailure "Expected 99"

  it "queryStreamP with list parameter" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStreamP p "UNWIND $xs AS x RETURN x"
            (H.singleton "xs" (PsList $ V.fromList [PsInteger 10, PsInteger 20, PsInteger 30]))
      Stream.fold Fold.toList s
    length result `shouldBe` 3
    let values = map (\r -> asInt (V.head r)) result
    values `shouldBe` [Just 10, Just 20, Just 30]


streamFoldTests :: TopSpec
streamFoldTests = describe "Stream fold operations" $ do

  it "fold with Fold.length counts records" $ do
    count <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 10) AS n RETURN n"
      Stream.fold Fold.length s
    count `shouldBe` 10

  it "fold with Fold.sum sums values" $ do
    total <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 5) AS n RETURN n"
      Stream.fold (Fold.lmap (\r -> maybe 0 id (asInt (V.head r) >>= fromPSInteger)) Fold.sum) s
    (total :: Int) `shouldBe` 15

  it "Stream.take limits records" $ do
    result <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 100) AS n RETURN n"
      Stream.fold Fold.toList $ Stream.take 3 s
    length result `shouldBe` 3


streamTransactionTests :: TopSpec
streamTransactionTests = describe "Streaming in transactions" $ do

  it "queryStream inside withTransaction" $ do
    result <- liftIO $ withConn $ \p ->
      withTransaction p $ \conn -> do
        s <- queryStream conn "UNWIND range(1, 5) AS n RETURN n"
        Stream.fold Fold.toList s
    length result `shouldBe` 5

  it "multiple queryStream calls in a transaction" $ do
    (r1, r2) <- liftIO $ withConn $ \p ->
      withTransaction p $ \conn -> do
        s1 <- queryStream conn "UNWIND [1,2,3] AS n RETURN n"
        res1 <- Stream.fold Fold.toList s1
        s2 <- queryStream conn "UNWIND [4,5,6] AS n RETURN n"
        res2 <- Stream.fold Fold.toList s2
        pure (res1, res2)
    length r1 `shouldBe` 3
    length r2 `shouldBe` 3

  it "queryStream with write in transaction" $ do
    liftIO $ withConn $ \p ->
      withTransaction p $ \conn -> do
        _ <- queryIO conn "CREATE (n:TestStreamTx {value: 1})"
        s <- queryStream conn "MATCH (n:TestStreamTx) RETURN n.value AS v"
        result <- Stream.fold Fold.toList s
        length result `shouldBe` 1
    -- Cleanup
    liftIO $ withConn $ \p -> queryIO p "MATCH (n:TestStreamTx) DELETE n" >> pure ()


largeResultTests :: TopSpec
largeResultTests = describe "Large result streaming" $ do

  it "streams 1000 rows without buffering all" $ do
    count <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 1000) AS n RETURN n"
      Stream.fold Fold.length s
    count `shouldBe` 1000

  it "streams 10000 rows" $ do
    count <- liftIO $ withConn $ \p -> do
      s <- queryStream p "UNWIND range(1, 10000) AS n RETURN n"
      Stream.fold Fold.length s
    count `shouldBe` 10000


-- ================================================================
-- Pool-based streaming tests
-- ================================================================

poolStreamTests :: TopSpec
poolStreamTests = describe "Pool-based streaming" $ do

  it "withPoolStream streams records" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    result <- liftIO $ withPoolStream pool "UNWIND range(1, 5) AS n RETURN n" $ \s ->
      Stream.fold Fold.toList s
    length result `shouldBe` 5
    liftIO $ destroyPool pool

  it "withPoolStreamP passes parameters" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    result <- liftIO $
      withPoolStreamP pool "RETURN $x AS n" (H.singleton "x" (PsInteger 42)) $ \s ->
        Stream.fold Fold.toList s
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 42
      Nothing -> expectationFailure "Expected 42"
    liftIO $ destroyPool pool

  it "withPoolStream works with empty result" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    result <- liftIO $ withPoolStream pool "UNWIND [] AS n RETURN n" $ \s ->
      Stream.fold Fold.toList s
    length result `shouldBe` 0
    liftIO $ destroyPool pool

  it "withPoolStream with fold length" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    count <- liftIO $ withPoolStream pool "UNWIND range(1, 100) AS n RETURN n" $ \s ->
      Stream.fold Fold.length s
    count `shouldBe` 100
    liftIO $ destroyPool pool


-- ================================================================
-- Routing pool streaming tests
-- ================================================================

routingStreamTests :: TopSpec
routingStreamTests = describe "Routing pool streaming" $ do

  it "withRoutingStream ReadAccess streams records" $ do
    cfg <- liftIO getRoutingConfig
    rp <- liftIO $ createRoutingPool cfg defaultRoutingPoolConfig
    result <- liftIO $
      withRoutingStream rp ReadAccess "UNWIND range(1, 5) AS n RETURN n" $ \s ->
        Stream.fold Fold.toList s
    length result `shouldBe` 5
    liftIO $ destroyRoutingPool rp

  it "withRoutingStream WriteAccess creates data" $ do
    cfg <- liftIO getRoutingConfig
    rp <- liftIO $ createRoutingPool cfg defaultRoutingPoolConfig
    liftIO $ withRoutingStream rp WriteAccess
      "CREATE (n:TestRoutingStream {value: 1}) RETURN n.value AS v" $ \s ->
        Stream.fold Fold.toList s
    -- Verify data was created
    result <- liftIO $
      withRoutingStream rp ReadAccess
        "MATCH (n:TestRoutingStream) RETURN n.value AS v" $ \s ->
          Stream.fold Fold.toList s
    V.length (V.fromList result) `shouldBe` 1
    -- Cleanup
    liftIO $ withRoutingConnection rp WriteAccess $ \p ->
      queryIO p "MATCH (n:TestRoutingStream) DELETE n" >> pure ()
    liftIO $ destroyRoutingPool rp

  it "withRoutingStreamP passes parameters" $ do
    cfg <- liftIO getRoutingConfig
    rp <- liftIO $ createRoutingPool cfg defaultRoutingPoolConfig
    result <- liftIO $
      withRoutingStreamP rp ReadAccess "RETURN $x AS n"
        (H.singleton "x" (PsInteger 99)) $ \s ->
          Stream.fold Fold.toList s
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 99
      Nothing -> expectationFailure "Expected 99"
    liftIO $ destroyRoutingPool rp

  it "withRoutingStream handles large results" $ do
    cfg <- liftIO getRoutingConfig
    rp <- liftIO $ createRoutingPool cfg defaultRoutingPoolConfig
    count <- liftIO $
      withRoutingStream rp ReadAccess "UNWIND range(1, 1000) AS n RETURN n" $ \s ->
        Stream.fold Fold.length s
    count `shouldBe` 1000
    liftIO $ destroyRoutingPool rp


-- ================================================================
-- Session streaming tests
-- ================================================================

sessionStreamTests :: TopSpec
sessionStreamTests = describe "Session streaming" $ do

  it "sessionReadStream reads data" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    session <- liftIO $ createSession pool defaultSessionConfig
    result <- liftIO $
      sessionReadStream session "UNWIND range(1, 5) AS n RETURN n" $ \s ->
        Stream.fold Fold.toList s
    length result `shouldBe` 5
    liftIO $ destroyPool pool

  it "sessionReadStreamP passes parameters" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    session <- liftIO $ createSession pool defaultSessionConfig
    result <- liftIO $
      sessionReadStreamP session "RETURN $x AS n" (H.singleton "x" (PsInteger 42)) $ \s ->
        Stream.fold Fold.toList s
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 42
      Nothing -> expectationFailure "Expected 42"
    liftIO $ destroyPool pool

  it "sessionWriteStream creates data and produces bookmark" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    session <- liftIO $ createSession pool defaultSessionConfig
    bms0 <- liftIO $ getLastBookmarks session
    bms0 `shouldBe` []
    liftIO $ sessionWriteStream session
      "CREATE (n:TestSessionStream {value: 1}) RETURN n.value AS v" $ \s ->
        Stream.fold Fold.toList s
    bms1 <- liftIO $ getLastBookmarks session
    length bms1 `shouldBe` 1
    -- Cleanup
    liftIO $ withConnection pool $ \p ->
      queryIO p "MATCH (n:TestSessionStream) DELETE n" >> pure ()
    liftIO $ destroyPool pool

  it "sessionWriteStreamP with parameters" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    session <- liftIO $ createSession pool defaultSessionConfig
    liftIO $ sessionWriteStreamP session
      "CREATE (n:TestSessionStreamP {value: $v}) RETURN n.value AS v"
      (H.singleton "v" (PsInteger 77)) $ \s ->
        Stream.fold Fold.toList s
    -- Verify
    result <- liftIO $ sessionReadStream session
      "MATCH (n:TestSessionStreamP) RETURN n.value AS v" $ \s ->
        Stream.fold Fold.toList s
    case asInt (V.head (head result)) of
      Just n  -> n `shouldBe` 77
      Nothing -> expectationFailure "Expected 77"
    -- Cleanup
    liftIO $ withConnection pool $ \p ->
      queryIO p "MATCH (n:TestSessionStreamP) DELETE n" >> pure ()
    liftIO $ destroyPool pool


-- ================================================================
-- Routing session streaming tests
-- ================================================================

routingSessionStreamTests :: TopSpec
routingSessionStreamTests = describe "Routing session streaming" $ do

  it "routing sessionReadStream works" $ do
    cfg <- liftIO getRoutingConfig
    rp <- liftIO $ createRoutingPool cfg defaultRoutingPoolConfig
    session <- liftIO $ createRoutingSession rp defaultSessionConfig
    result <- liftIO $
      sessionReadStream session "UNWIND range(1, 3) AS n RETURN n" $ \s ->
        Stream.fold Fold.toList s
    length result `shouldBe` 3
    liftIO $ destroyRoutingPool rp

  it "routing sessionWriteStream produces bookmark" $ do
    cfg <- liftIO getRoutingConfig
    rp <- liftIO $ createRoutingPool cfg defaultRoutingPoolConfig
    session <- liftIO $ createRoutingSession rp defaultSessionConfig
    liftIO $ sessionWriteStream session
      "CREATE (n:TestRoutingSessStream {value: 1}) RETURN n.value AS v" $ \s ->
        Stream.fold Fold.toList s
    bms <- liftIO $ getLastBookmarks session
    length bms `shouldBe` 1
    -- Cleanup
    liftIO $ withRoutingConnection rp WriteAccess $ \p ->
      queryIO p "MATCH (n:TestRoutingSessStream) DELETE n" >> pure ()
    liftIO $ destroyRoutingPool rp


-- ================================================================
-- Streaming decode tests
-- ================================================================

streamingDecodeTests :: TopSpec
streamingDecodeTests = describe "Streaming decode" $ do

  it "queryStreamAs decodes RETURN 1 AS n with column 0 int64" $ do
    result <- liftIO $ withConn $ \conn -> do
      s <- queryStreamAs (column 0 int64) conn "RETURN 1 AS n"
      Stream.fold Fold.toList s
    result `shouldBe` [1 :: Int64]

  it "queryStreamPAs decodes with parameters" $ do
    result <- liftIO $ withConn $ \conn -> do
      s <- queryStreamPAs (column 0 int64) conn "RETURN $x AS n"
            (H.singleton "x" (PsInteger 42))
      Stream.fold Fold.toList s
    result `shouldBe` [42 :: Int64]

  it "queryStreamAs with field name lookup" $ do
    result <- liftIO $ withConn $ \conn -> do
      s <- queryStreamAs (field "name" text) conn "RETURN 'hello' AS name"
      Stream.fold Fold.toList s
    result `shouldBe` [T.pack "hello"]

  it "queryStreamAs with multi-column Applicative decoder" $ do
    let decoder = (,) <$> field "a" text <*> field "b" int64
    result <- liftIO $ withConn $ \conn -> do
      s <- queryStreamAs decoder conn "UNWIND [{a: 'x', b: 1}, {a: 'y', b: 2}] AS row RETURN row.a AS a, row.b AS b"
      Stream.fold Fold.toList s
    result `shouldBe` [(T.pack "x", 1), (T.pack "y", 2)]

  it "queryStreamAs throws DecodeError on type mismatch" $ do
    result <- liftIO $ E.try @DecodeError $ withConn $ \conn -> do
      s <- queryStreamAs (column 0 int64) conn "RETURN 'not_a_number' AS n"
      Stream.fold Fold.toList s
    case result of
      Left (TypeMismatch _ _) -> pure ()
      Left err -> expectationFailure $ "Expected TypeMismatch, got: " <> show err
      Right _ -> expectationFailure "Expected DecodeError"

  it "withPoolStreamAs decodes through pool" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    result <- liftIO $ withPoolStreamAs (column 0 int64) pool "RETURN 99 AS n" $ \s ->
      Stream.fold Fold.toList s
    result `shouldBe` [99 :: Int64]
    liftIO $ destroyPool pool

  it "sessionReadStreamAs decodes through session" $ do
    cfg <- liftIO getConfig
    pool <- liftIO $ createPool cfg defaultPoolConfig
    session <- liftIO $ createSession pool defaultSessionConfig
    result <- liftIO $
      sessionReadStreamAs (column 0 int64) session "RETURN 7 AS n" $ \s ->
        Stream.fold Fold.toList s
    result `shouldBe` [7 :: Int64]
    liftIO $ destroyPool pool


main :: IO ()
main = runSandwichWithCommandLineArgs defaultOptions $ do
  streamingTests
  streamFoldTests
  streamTransactionTests
  largeResultTests
  poolStreamTests
  routingStreamTests
  sessionStreamTests
  routingSessionStreamTests
  streamingDecodeTests
