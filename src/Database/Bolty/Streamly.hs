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
  ( -- * Streaming queries
    queryStream
  , queryStreamP
    -- * Low-level streaming
  , pullStream
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

import           Database.Bolty.Connection     (requestResponseRun)
import           Database.Bolty.Connection.Pipe (flush, fetch, requireState,
                                                  getState, setState, reset, MonadPipe)
import           Database.Bolty.Connection.Type
import           Database.Bolty.Message.Request (Request(..), defaultPull)
import           Database.Bolty.Message.Response (Response(..), Failure(..))
import           Database.Bolty.Record         (Record)


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
