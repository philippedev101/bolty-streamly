-- | Streamly streaming interface for bolty Neo4j queries.
--
-- Instead of buffering all result records into a 'V.Vector', this module
-- yields records one-by-one as a @'Stream' IO 'Record'@,
-- allowing constant-memory consumption of large result sets.
--
-- Streams returned by pool, routing, and session functions manage connection
-- lifetime automatically via @bracketIO@ — the connection is acquired when
-- the stream starts and released when it finishes, errors, or is garbage
-- collected. Streams are ordinary values that can be stored, composed,
-- and passed to other functions.
--
-- @
-- import qualified Database.Bolty           as Bolt
-- import qualified Database.Bolty.Streamly  as BoltS
-- import qualified Streamly.Data.Stream     as Stream
-- import qualified Streamly.Data.Fold       as Fold
--
-- main :: IO ()
-- main = do
--   pool <- Bolt.createPool cfg Bolt.defaultPoolConfig
--   let s = BoltS.poolStream pool \"MATCH (n) RETURN n\"
--   count <- Stream.fold Fold.length s
--   Bolt.destroyPool pool
-- @
module Database.Bolty.Streamly
  ( -- * Streaming queries (bare connection)
    queryStream
  , queryStreamP
  , queryStreamAs
  , queryStreamPAs
    -- * Low-level streaming
  , pullStream
    -- * Pool-based streaming
  , poolStream
  , poolStreamP
  , poolStreamAs
  , poolStreamPAs
    -- * Routing pool streaming
  , routingStream
  , routingStreamP
  , routingStreamAs
  , routingStreamPAs
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

import           Control.Exception              (throwIO, SomeException, try)
import           Data.Kind                      (Type)
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
import           Database.Bolty.Message.Request (Request(..), defaultPull, Begin(Begin), TelemetryApi(..))
import           Database.Bolty.Message.Response (Response(..), Failure(..), successFields)
import           Database.Bolty.Pool            (BoltPool, CheckedOutConnection(..),
                                                  acquireConnection, releaseConnection,
                                                  releaseConnectionOnError)
import           Database.Bolty.Record          (Record)
import           Database.Bolty.Routing         (AccessMode(..), RoutingPool,
                                                  acquireRoutingConnection)
import           Database.Bolty.Session         (Session(..), SessionPool(..), SessionConfig(..),
                                                  BookmarkManager, getBookmarks, updateBookmark)

import           Control.Monad                  (when)
import           Data.IORef                     (IORef, readIORef, writeIORef)


-- ---------------------------------------------------------------------------
-- Bare connection streaming (unchanged)
-- ---------------------------------------------------------------------------

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
type PullState :: Type
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
            P.flushIO c $ RPull defaultPull
            step c NeedPull
          else do
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
-- Pool-based streaming (bracketIO)
-- ---------------------------------------------------------------------------

-- | Run a streaming query using a pooled connection. The connection is
-- checked out when the stream starts and returned when it finishes.
poolStream :: HasCallStack => BoltPool -> Text -> Stream IO Record
poolStream pool cypher = poolStreamP pool cypher H.empty


-- | Like 'poolStream' but with query parameters.
poolStreamP :: HasCallStack => BoltPool -> Text -> H.HashMap Text PS.Ps -> Stream IO Record
poolStreamP pool cypher params =
  Stream.bracketIO
    (acquireConnection pool)
    releaseConnection
    (\coc -> Stream.concatEffect $ queryStreamP (cocConnection coc) cypher params)


-- | Like 'poolStream' but decodes each record using a 'RowDecoder'.
poolStreamAs :: HasCallStack => RowDecoder a -> BoltPool -> Text -> Stream IO a
poolStreamAs decoder pool cypher = poolStreamPAs decoder pool cypher H.empty


-- | Like 'poolStreamP' but decodes each record using a 'RowDecoder'.
poolStreamPAs :: HasCallStack => RowDecoder a -> BoltPool -> Text -> H.HashMap Text PS.Ps -> Stream IO a
poolStreamPAs decoder pool cypher params =
  Stream.bracketIO
    (acquireConnection pool)
    releaseConnection
    (\coc -> Stream.concatEffect $ queryStreamPAs decoder (cocConnection coc) cypher params)


-- ---------------------------------------------------------------------------
-- Routing pool streaming (bracketIO)
-- ---------------------------------------------------------------------------

-- | Run a streaming query using a routing pool. Automatically selects
-- a server based on the access mode (read replica or leader).
routingStream :: HasCallStack => RoutingPool -> AccessMode -> Text -> Stream IO Record
routingStream rp mode cypher = routingStreamP rp mode cypher H.empty


-- | Like 'routingStream' but with query parameters.
routingStreamP :: HasCallStack
               => RoutingPool -> AccessMode -> Text -> H.HashMap Text PS.Ps -> Stream IO Record
routingStreamP rp mode cypher params =
  Stream.bracketIO
    (acquireRoutingConnection rp mode)
    releaseConnection
    (\coc -> Stream.concatEffect $ queryStreamP (cocConnection coc) cypher params)


-- | Like 'routingStream' but decodes each record using a 'RowDecoder'.
routingStreamAs :: HasCallStack => RowDecoder a -> RoutingPool -> AccessMode -> Text -> Stream IO a
routingStreamAs decoder rp mode cypher = routingStreamPAs decoder rp mode cypher H.empty


-- | Like 'routingStreamP' but decodes each record using a 'RowDecoder'.
routingStreamPAs :: HasCallStack
                 => RowDecoder a -> RoutingPool -> AccessMode -> Text -> H.HashMap Text PS.Ps -> Stream IO a
routingStreamPAs decoder rp mode cypher params =
  Stream.bracketIO
    (acquireRoutingConnection rp mode)
    releaseConnection
    (\coc -> Stream.concatEffect $ queryStreamPAs decoder (cocConnection coc) cypher params)


-- ---------------------------------------------------------------------------
-- Session streaming (bracketIO with BEGIN/COMMIT)
-- ---------------------------------------------------------------------------

-- | Run a streaming read query inside a managed transaction.
-- Handles BEGIN, COMMIT, bookmark propagation, and connection lifecycle.
sessionReadStream :: HasCallStack => Session -> Text -> Stream IO Record
sessionReadStream session cypher = sessionReadStreamP session cypher H.empty


-- | Like 'sessionReadStream' but with query parameters.
sessionReadStreamP :: HasCallStack => Session -> Text -> H.HashMap Text PS.Ps -> Stream IO Record
sessionReadStreamP session cypher params =
  sessionStreamRaw session ReadAccess cypher params


-- | Run a streaming write query inside a managed transaction.
sessionWriteStream :: HasCallStack => Session -> Text -> Stream IO Record
sessionWriteStream session cypher = sessionWriteStreamP session cypher H.empty


-- | Like 'sessionWriteStream' but with query parameters.
sessionWriteStreamP :: HasCallStack => Session -> Text -> H.HashMap Text PS.Ps -> Stream IO Record
sessionWriteStreamP session cypher params =
  sessionStreamRaw session WriteAccess cypher params


-- | Like 'sessionReadStream' but decodes each record using a 'RowDecoder'.
sessionReadStreamAs :: HasCallStack => RowDecoder a -> Session -> Text -> Stream IO a
sessionReadStreamAs decoder session cypher =
  sessionReadStreamPAs decoder session cypher H.empty


-- | Like 'sessionReadStreamP' but decodes each record using a 'RowDecoder'.
sessionReadStreamPAs :: HasCallStack
                     => RowDecoder a -> Session -> Text -> H.HashMap Text PS.Ps -> Stream IO a
sessionReadStreamPAs decoder session cypher params =
  sessionStreamDecoded decoder session ReadAccess cypher params


-- | Like 'sessionWriteStream' but decodes each record using a 'RowDecoder'.
sessionWriteStreamAs :: HasCallStack => RowDecoder a -> Session -> Text -> Stream IO a
sessionWriteStreamAs decoder session cypher =
  sessionWriteStreamPAs decoder session cypher H.empty


-- | Like 'sessionWriteStreamP' but decodes each record using a 'RowDecoder'.
sessionWriteStreamPAs :: HasCallStack
                      => RowDecoder a -> Session -> Text -> H.HashMap Text PS.Ps -> Stream IO a
sessionWriteStreamPAs decoder session cypher params =
  sessionStreamDecoded decoder session WriteAccess cypher params


-- | Internal: session streaming for raw records.
sessionStreamRaw :: HasCallStack
                 => Session -> AccessMode -> Text -> H.HashMap Text PS.Ps -> Stream IO Record
sessionStreamRaw Session{sPool, sBookmarks, sConfig, sTelemetrySent} mode cypher params =
  Stream.bracketIO (sessionAcquire sPool sBookmarks sConfig sTelemetrySent mode)
                   (sessionCleanup sBookmarks sTelemetrySent)
                   (\coc -> Stream.concatEffect $ queryStreamP (cocConnection coc) cypher params)


-- | Internal: session streaming with record decoding.
sessionStreamDecoded :: HasCallStack
                     => RowDecoder a -> Session -> AccessMode -> Text -> H.HashMap Text PS.Ps -> Stream IO a
sessionStreamDecoded decoder Session{sPool, sBookmarks, sConfig, sTelemetrySent} mode cypher params =
  Stream.bracketIO (sessionAcquire sPool sBookmarks sConfig sTelemetrySent mode)
                   (sessionCleanup sBookmarks sTelemetrySent)
                   (\coc -> Stream.concatEffect $ queryStreamPAs decoder (cocConnection coc) cypher params)


-- | Acquire a session connection and begin a transaction.
sessionAcquire :: SessionPool -> BookmarkManager -> SessionConfig -> IORef Bool
               -> AccessMode -> IO CheckedOutConnection
sessionAcquire sPool sBookmarks sConfig _telRef mode = do
  let modeChar = case mode of { ReadAccess -> 'r'; WriteAccess -> 'w' }
  let db = sessionDatabase sConfig
  coc <- acquireSessionConnection sPool mode
  let conn = cocConnection coc
  bms <- getBookmarks sBookmarks
  P.beginTx conn $ Begin (V.fromList bms) Nothing H.empty modeChar db Nothing
  pure coc


-- | Commit the transaction and release the connection.
sessionCleanup :: BookmarkManager -> IORef Bool -> CheckedOutConnection -> IO ()
sessionCleanup sBookmarks sTelemetrySent coc = do
  let conn = cocConnection coc
  result <- try $ do
    mbBookmark <- P.commitTx conn
    case mbBookmark of
      Just bm -> updateBookmark sBookmarks bm
      Nothing -> pure ()
    sent <- readIORef sTelemetrySent
    when (not sent) $ do
      writeIORef sTelemetrySent True
      P.sendTelemetry conn ManagedTransactions
  case result of
    Right () -> releaseConnection coc
    Left (_ :: SomeException) -> releaseConnectionOnError coc


-- | Acquire a connection based on session pool type.
acquireSessionConnection :: SessionPool -> AccessMode -> IO CheckedOutConnection
acquireSessionConnection (DirectPool pool) _mode = acquireConnection pool
acquireSessionConnection (RoutedPool rp) mode = acquireRoutingConnection rp mode


-- | Extract the database field from SessionConfig (avoids ambiguous record access).
sessionDatabase :: SessionConfig -> Maybe Text
sessionDatabase SessionConfig{database} = database
