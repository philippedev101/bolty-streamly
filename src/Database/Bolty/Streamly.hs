{-# LANGUAGE OverloadedStrings #-}

-- | Streamly streaming interface for bolty Neo4j queries.
--
-- Instead of buffering all result records into a 'V.Vector', this module
-- yields records one-by-one as a @'Stream' IO 'Record'@,
-- allowing constant-memory consumption of large result sets.
--
-- @
-- import qualified Database.Bolty           as Bolt
-- import qualified Database.Bolty.Streamly  as BoltS
-- import qualified Streamly.Data.Stream     as Stream
-- import qualified Streamly.Data.Fold       as Fold
--
-- main :: IO ()
-- main = do
--   conn <- Bolt.connect cfg
--   s <- BoltS.queryStream conn \"MATCH (n) RETURN n\"
--   count <- Stream.fold Fold.length s
--   Bolt.close conn
-- @
module Database.Bolty.Streamly
  ( -- * Streaming queries
    queryStream
  , queryStreamP
    -- * Streaming queries with decoding
  , queryStreamAs
  , queryStreamPAs
    -- * Low-level streaming
  , pullStream
    -- * Pool-based streaming
  , withPoolStream
  , withPoolStreamP
  , withPoolStreamAs
  , withPoolStreamPAs
    -- * Routing pool streaming
  , withRoutingStream
  , withRoutingStreamP
  , withRoutingStreamAs
  , withRoutingStreamPAs
    -- * Session streaming
  , sessionReadStream
  , sessionReadStreamP
  , sessionWriteStream
  , sessionWriteStreamP
  , sessionReadStreamAs
  , sessionReadStreamPAs
  , sessionWriteStreamAs
  , sessionWriteStreamPAs
    -- * Re-exports
  , Stream
  ) where

import           Control.Exception              (throwIO)
import           Data.Text                      (Text)
import           GHC.Stack                      (HasCallStack)
import qualified Data.HashMap.Lazy              as H
import qualified Data.PackStream.Ps             as PS
import           Data.PackStream.Result         (Result(..))
import qualified Data.Vector                    as V
import           Streamly.Data.Stream           (Stream)
import qualified Streamly.Data.Stream           as Stream

import           Database.Bolty.Connection      (requestResponseRunIO)
import qualified Database.Bolty.Connection.Pipe as P
import           Database.Bolty.Connection.Type
import           Database.Bolty.Decode          (RowDecoder, decodeRow)
import           Database.Bolty.Message.Request (Request(..), defaultPull)
import           Database.Bolty.Message.Response (Response(..), Failure(..), successFields)
import           Database.Bolty.Pool            (BoltPool, withConnection)
import           Database.Bolty.Record          (Record)
import           Database.Bolty.Routing         (AccessMode(..), RoutingPool,
                                                  withRoutingConnection)
import           Database.Bolty.Session         (Session, readTransaction, writeTransaction)


-- | Run a Cypher query and return results as a stream of records.
--
-- Records are yielded one at a time as they arrive from the server,
-- without buffering the entire result set in memory.
--
-- Must be called in @Ready@ or @TXready@ state.
queryStream :: HasCallStack => Connection -> Text -> IO (Stream IO Record)
queryStream conn cypher = queryStreamP conn cypher H.empty


-- | Run a parameterised Cypher query and return results as a stream.
queryStreamP :: HasCallStack => Connection -> Text -> H.HashMap Text PS.Ps -> IO (Stream IO Record)
queryStreamP conn cypher params = do
  _ <- requestResponseRunIO conn cypher params
  pullStream conn


-- | Run a Cypher query and decode each record using a 'RowDecoder'.
-- Throws 'Database.Bolty.Decode.DecodeError' on decode failure.
queryStreamAs :: HasCallStack => RowDecoder a -> Connection -> Text -> IO (Stream IO a)
queryStreamAs decoder conn cypher = queryStreamPAs decoder conn cypher H.empty


-- | Run a parameterised Cypher query and decode each record using a 'RowDecoder'.
-- Throws 'Database.Bolty.Decode.DecodeError' on decode failure.
queryStreamPAs :: HasCallStack => RowDecoder a -> Connection -> Text -> H.HashMap Text PS.Ps -> IO (Stream IO a)
queryStreamPAs decoder conn cypher params = do
  runResp <- requestResponseRunIO conn cypher params
  let columns = successFields runResp
  s <- pullStream conn
  pure $ Stream.mapM (decodeOrThrow decoder columns) s


-- | Decode a single record, throwing 'Database.Bolty.Decode.DecodeError' on failure.
decodeOrThrow :: RowDecoder a -> V.Vector Text -> Record -> IO a
decodeOrThrow decoder columns record =
  case decodeRow decoder columns record of
    Right a  -> pure a
    Left err -> throwIO err


-- | Pull state machine.  @NeedPull@ means we need to send a new PULL
-- message to the server.  @Done@ means the result set is exhausted.
data PullState = NeedPull | Done


-- | Stream records from an in-progress PULL.
--
-- Expects the connection to be in @Streaming@ or @TXstreaming@ state
-- (i.e. after a RUN has been sent and acknowledged). Sends PULL messages
-- and yields each 'Record' as it arrives.  When the server signals
-- completion, the state transitions back to @Ready@ / @TXready@.
pullStream :: HasCallStack => Connection -> IO (Stream IO Record)
pullStream conn = do
  P.requireStateIO conn [Streaming, TXstreaming] "PULL"
  P.flushIO conn $ RPull defaultPull
  pure $ Stream.unfoldrM (step conn) NeedPull
  where
    step :: HasCallStack => Connection -> PullState -> IO (Maybe (Record, PullState))
    step _ Done = pure Nothing
    step c NeedPull = do
      response <- P.fetchIO c
      case response of
        RRecord record ->
          pure $ Just (record, NeedPull)

        RSuccess meta -> do
          let hasMore = case H.lookup "has_more" meta of
                          Just hm -> case PS.fromPs hm of
                            Success True -> True
                            _            -> False
                          Nothing -> False
          if hasMore then do
            -- Server has more batches; send another PULL and continue
            P.flushIO c $ RPull defaultPull
            step c NeedPull
          else do
            -- All records consumed; transition state
            st <- P.getState c
            P.setState c $ case st of
              TXstreaming -> TXready
              _           -> Ready
            pure Nothing

        RIgnored -> do
          P.reset c
          throwIO ResponseErrorIgnored

        RFailure Failure{code, message} -> do
          P.setState c Failed
          P.reset c
          throwIO $ ResponseErrorFailure code message


-- ---------------------------------------------------------------------------
-- Pool-based streaming
-- ---------------------------------------------------------------------------

-- | Acquire a connection from a pool, run a streaming query, and pass the
-- stream to the consumer function. The connection is held until the consumer
-- returns. The stream must be fully consumed within the consumer.
withPoolStream :: HasCallStack
              => BoltPool
              -> Text
              -> (Stream IO Record -> IO a)
              -> IO a
withPoolStream pool cypher consume =
  withPoolStreamP pool cypher H.empty consume


-- | Like 'withPoolStream' but with query parameters.
withPoolStreamP :: HasCallStack
               => BoltPool
               -> Text
               -> H.HashMap Text PS.Ps
               -> (Stream IO Record -> IO a)
               -> IO a
withPoolStreamP pool cypher params consume =
  withConnection pool $ \conn -> do
    s <- queryStreamP conn cypher params
    consume s


-- | Like 'withPoolStream' but decodes each record using a 'RowDecoder'.
withPoolStreamAs :: HasCallStack
                => RowDecoder a
                -> BoltPool
                -> Text
                -> (Stream IO a -> IO b)
                -> IO b
withPoolStreamAs decoder pool cypher consume =
  withPoolStreamPAs decoder pool cypher H.empty consume


-- | Like 'withPoolStreamP' but decodes each record using a 'RowDecoder'.
withPoolStreamPAs :: HasCallStack
                 => RowDecoder a
                 -> BoltPool
                 -> Text
                 -> H.HashMap Text PS.Ps
                 -> (Stream IO a -> IO b)
                 -> IO b
withPoolStreamPAs decoder pool cypher params consume =
  withConnection pool $ \conn -> do
    s <- queryStreamPAs decoder conn cypher params
    consume s


-- ---------------------------------------------------------------------------
-- Routing pool streaming
-- ---------------------------------------------------------------------------

-- | Acquire a routed connection, run a streaming query, and pass the stream
-- to the consumer. Uses 'ReadAccess' or 'WriteAccess' to direct queries
-- to the appropriate cluster member.
withRoutingStream :: HasCallStack
                 => RoutingPool
                 -> AccessMode
                 -> Text
                 -> (Stream IO Record -> IO a)
                 -> IO a
withRoutingStream rp mode cypher consume =
  withRoutingStreamP rp mode cypher H.empty consume


-- | Like 'withRoutingStream' but with query parameters.
withRoutingStreamP :: HasCallStack
                  => RoutingPool
                  -> AccessMode
                  -> Text
                  -> H.HashMap Text PS.Ps
                  -> (Stream IO Record -> IO a)
                  -> IO a
withRoutingStreamP rp mode cypher params consume =
  withRoutingConnection rp mode $ \conn -> do
    s <- queryStreamP conn cypher params
    consume s


-- | Like 'withRoutingStream' but decodes each record using a 'RowDecoder'.
withRoutingStreamAs :: HasCallStack
                   => RowDecoder a
                   -> RoutingPool
                   -> AccessMode
                   -> Text
                   -> (Stream IO a -> IO b)
                   -> IO b
withRoutingStreamAs decoder rp mode cypher consume =
  withRoutingStreamPAs decoder rp mode cypher H.empty consume


-- | Like 'withRoutingStreamP' but decodes each record using a 'RowDecoder'.
withRoutingStreamPAs :: HasCallStack
                    => RowDecoder a
                    -> RoutingPool
                    -> AccessMode
                    -> Text
                    -> H.HashMap Text PS.Ps
                    -> (Stream IO a -> IO b)
                    -> IO b
withRoutingStreamPAs decoder rp mode cypher params consume =
  withRoutingConnection rp mode $ \conn -> do
    s <- queryStreamPAs decoder conn cypher params
    consume s


-- ---------------------------------------------------------------------------
-- Session streaming
-- ---------------------------------------------------------------------------

-- | Run a streaming query inside a managed read transaction.
-- Handles BEGIN, COMMIT, bookmark propagation, and retries on transient
-- failures. Directs queries to read replicas when using a routing session.
sessionReadStream :: HasCallStack
                 => Session
                 -> Text
                 -> (Stream IO Record -> IO a)
                 -> IO a
sessionReadStream session cypher consume =
  sessionReadStreamP session cypher H.empty consume


-- | Like 'sessionReadStream' but with query parameters.
sessionReadStreamP :: HasCallStack
                  => Session
                  -> Text
                  -> H.HashMap Text PS.Ps
                  -> (Stream IO Record -> IO a)
                  -> IO a
sessionReadStreamP session cypher params consume =
  readTransaction session $ \conn -> do
    s <- queryStreamP conn cypher params
    consume s


-- | Run a streaming query inside a managed write transaction.
-- Handles BEGIN, COMMIT, bookmark propagation, and retries on transient
-- failures. Directs queries to the leader when using a routing session.
sessionWriteStream :: HasCallStack
                  => Session
                  -> Text
                  -> (Stream IO Record -> IO a)
                  -> IO a
sessionWriteStream session cypher consume =
  sessionWriteStreamP session cypher H.empty consume


-- | Like 'sessionWriteStream' but with query parameters.
sessionWriteStreamP :: HasCallStack
                   => Session
                   -> Text
                   -> H.HashMap Text PS.Ps
                   -> (Stream IO Record -> IO a)
                   -> IO a
sessionWriteStreamP session cypher params consume =
  writeTransaction session $ \conn -> do
    s <- queryStreamP conn cypher params
    consume s


-- | Like 'sessionReadStream' but decodes each record using a 'RowDecoder'.
sessionReadStreamAs :: HasCallStack
                   => RowDecoder a
                   -> Session
                   -> Text
                   -> (Stream IO a -> IO b)
                   -> IO b
sessionReadStreamAs decoder session cypher consume =
  sessionReadStreamPAs decoder session cypher H.empty consume


-- | Like 'sessionReadStreamP' but decodes each record using a 'RowDecoder'.
sessionReadStreamPAs :: HasCallStack
                    => RowDecoder a
                    -> Session
                    -> Text
                    -> H.HashMap Text PS.Ps
                    -> (Stream IO a -> IO b)
                    -> IO b
sessionReadStreamPAs decoder session cypher params consume =
  readTransaction session $ \conn -> do
    s <- queryStreamPAs decoder conn cypher params
    consume s


-- | Like 'sessionWriteStream' but decodes each record using a 'RowDecoder'.
sessionWriteStreamAs :: HasCallStack
                    => RowDecoder a
                    -> Session
                    -> Text
                    -> (Stream IO a -> IO b)
                    -> IO b
sessionWriteStreamAs decoder session cypher consume =
  sessionWriteStreamPAs decoder session cypher H.empty consume


-- | Like 'sessionWriteStreamP' but decodes each record using a 'RowDecoder'.
sessionWriteStreamPAs :: HasCallStack
                     => RowDecoder a
                     -> Session
                     -> Text
                     -> H.HashMap Text PS.Ps
                     -> (Stream IO a -> IO b)
                     -> IO b
sessionWriteStreamPAs decoder session cypher params consume =
  writeTransaction session $ \conn -> do
    s <- queryStreamPAs decoder conn cypher params
    consume s
