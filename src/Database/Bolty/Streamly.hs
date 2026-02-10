{-# LANGUAGE OverloadedStrings #-}

-- | Streamly streaming interface for bolty Neo4j queries.
--
-- Instead of buffering all result records into a 'V.Vector', this module
-- yields records one-by-one as a @'Stream' ('BoltActionT' m) 'Record'@,
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
--   pipe <- Bolt.connect cfg
--   Bolt.run pipe $ do
--     Bolt.begin
--     count <- BoltS.queryStream \"MATCH (n) RETURN n\"
--                & Stream.fold Fold.length
--     Bolt.commit
--   Bolt.close pipe
-- @
module Database.Bolty.Streamly
  ( -- * Streaming queries (within BoltActionT)
    queryStream
  , queryStreamP
    -- * Low-level streaming
  , pullStream
    -- * Pool-based streaming
  , withPoolStream
  , withPoolStreamP
    -- * Routing pool streaming
  , withRoutingStream
  , withRoutingStreamP
    -- * Session streaming
  , sessionReadStream
  , sessionReadStreamP
  , sessionWriteStream
  , sessionWriteStreamP
    -- * Re-exports
  , Stream
  ) where

import           Control.Monad.Except          (MonadError(..))
import           Control.Monad.Reader          (MonadReader(..))
import           Control.Monad.Trans           (MonadIO(..))
import           Data.Text                     (Text)
import           GHC.Stack                     (HasCallStack)
import qualified Data.HashMap.Lazy             as H
import qualified Data.PackStream.Ps            as PS
import           Data.PackStream.Result        (Result(..))
import           Streamly.Data.Stream          (Stream)
import qualified Streamly.Data.Stream          as Stream

import           Control.Exception             (throwIO)
import           Control.Monad.Except          (runExceptT)
import           Control.Monad.Reader          (runReaderT)

import           Database.Bolty.Connection     (requestResponseRun)
import           Database.Bolty.Connection.Pipe (flush, fetch, requireState,
                                                  getState, setState, reset)
import           Database.Bolty.Connection.Type
import           Database.Bolty.Message.Request (Request(..), defaultPull)
import           Database.Bolty.Message.Response (Response(..), Failure(..))
import           Database.Bolty.Pool           (BoltPool, withConnection)
import           Database.Bolty.Record         (Record)
import           Database.Bolty.Routing        (AccessMode(..), RoutingPool,
                                                withRoutingConnection)
import           Database.Bolty.Session        (Session, readTransaction, writeTransaction)


-- | Run a Cypher query and return results as a stream of records.
--
-- Records are yielded one at a time as they arrive from the server,
-- without buffering the entire result set in memory.
--
-- Must be called in @Ready@ or @TXready@ state.
queryStream :: (MonadIO m, HasCallStack) => Text -> BoltActionT m (Stream (BoltActionT m) Record)
queryStream cypher = queryStreamP cypher H.empty


-- | Run a parameterised Cypher query and return results as a stream.
queryStreamP :: (MonadIO m, HasCallStack) => Text -> H.HashMap Text PS.Ps -> BoltActionT m (Stream (BoltActionT m) Record)
queryStreamP cypher params = do
  _ <- requestResponseRun cypher params
  pure pullStream


-- | Pull state machine.  @NeedPull@ means we need to send a new PULL
-- message to the server.  @Done@ means the result set is exhausted.
data PullState = NeedPull | Done


-- | Stream records from an in-progress PULL.
--
-- Expects the connection to be in @Streaming@ or @TXstreaming@ state
-- (i.e. after a RUN has been sent and acknowledged). Sends PULL messages
-- and yields each 'Record' as it arrives.  When the server signals
-- completion, the state transitions back to @Ready@ / @TXready@.
pullStream :: (MonadIO m, HasCallStack) => Stream (BoltActionT m) Record
pullStream = Stream.concatEffect $ do
  pipe <- ask
  liftE $ requireState pipe [Streaming, TXstreaming] "PULL"
  liftE $ flush pipe $ RPull defaultPull
  pure $ Stream.unfoldrM (step pipe) NeedPull
  where
    step :: (MonadIO m, HasCallStack) => Pipe -> PullState -> BoltActionT m (Maybe (Record, PullState))
    step _ Done = pure Nothing
    step pipe NeedPull = do
      response <- fetch pipe
      case response of
        RRecord record ->
          pure $ Just (record, NeedPull)

        RSuccess meta -> do
          let hasMore = case H.lookup "has_more" meta of
                          Just hm -> case PS.fromPs hm of
                            Success True -> True
                            _               -> False
                          Nothing -> False
          if hasMore then do
            -- Server has more batches; send another PULL and continue
            liftE $ flush pipe $ RPull defaultPull
            step pipe NeedPull
          else do
            -- All records consumed; transition state
            st <- liftE $ getState pipe
            liftE $ setState pipe $ case st of
              TXstreaming -> TXready
              _           -> Ready
            pure Nothing

        RIgnored -> do
          reset pipe
          throwError ResponseErrorIgnored

        RFailure Failure{code, message} -> do
          liftE $ setState pipe Failed
          reset pipe
          throwError $ ResponseErrorFailure code message


-- ---------------------------------------------------------------------------
-- Internal: run BoltActionT, throwing on error
-- ---------------------------------------------------------------------------

runBolt :: HasCallStack => Pipe -> BoltActionT IO a -> IO a
runBolt pipe action = do
  result <- runExceptT (runReaderT (runBoltActionT action) pipe)
  case result of
    Right x -> pure x
    Left  e -> throwIO e


-- ---------------------------------------------------------------------------
-- Pool-based streaming
-- ---------------------------------------------------------------------------

-- | Acquire a connection from a pool, run a streaming query, and pass the
-- stream to the consumer function. The connection is held until the consumer
-- returns. The stream must be fully consumed within the consumer.
withPoolStream :: HasCallStack
              => BoltPool
              -> Text
              -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
              -> IO a
withPoolStream pool cypher consume =
  withPoolStreamP pool cypher H.empty consume


-- | Like 'withPoolStream' but with query parameters.
withPoolStreamP :: HasCallStack
               => BoltPool
               -> Text
               -> H.HashMap Text PS.Ps
               -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
               -> IO a
withPoolStreamP pool cypher params consume =
  withConnection pool $ \pipe -> runBolt pipe $ do
    s <- queryStreamP cypher params
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
                 -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
                 -> IO a
withRoutingStream rp mode cypher consume =
  withRoutingStreamP rp mode cypher H.empty consume


-- | Like 'withRoutingStream' but with query parameters.
withRoutingStreamP :: HasCallStack
                  => RoutingPool
                  -> AccessMode
                  -> Text
                  -> H.HashMap Text PS.Ps
                  -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
                  -> IO a
withRoutingStreamP rp mode cypher params consume =
  withRoutingConnection rp mode $ \pipe -> runBolt pipe $ do
    s <- queryStreamP cypher params
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
                 -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
                 -> IO a
sessionReadStream session cypher consume =
  sessionReadStreamP session cypher H.empty consume


-- | Like 'sessionReadStream' but with query parameters.
sessionReadStreamP :: HasCallStack
                  => Session
                  -> Text
                  -> H.HashMap Text PS.Ps
                  -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
                  -> IO a
sessionReadStreamP session cypher params consume =
  readTransaction session $ do
    s <- queryStreamP cypher params
    consume s


-- | Run a streaming query inside a managed write transaction.
-- Handles BEGIN, COMMIT, bookmark propagation, and retries on transient
-- failures. Directs queries to the leader when using a routing session.
sessionWriteStream :: HasCallStack
                  => Session
                  -> Text
                  -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
                  -> IO a
sessionWriteStream session cypher consume =
  sessionWriteStreamP session cypher H.empty consume


-- | Like 'sessionWriteStream' but with query parameters.
sessionWriteStreamP :: HasCallStack
                   => Session
                   -> Text
                   -> H.HashMap Text PS.Ps
                   -> (Stream (BoltActionT IO) Record -> BoltActionT IO a)
                   -> IO a
sessionWriteStreamP session cypher params consume =
  writeTransaction session $ do
    s <- queryStreamP cypher params
    consume s
