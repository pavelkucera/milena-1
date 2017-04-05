{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE Rank2Types #-}

module Network.Kafka.Protocol
  ( module Network.Kafka.Protocol
  ) where

import Control.Applicative
import Control.Category (Category(..))
import Control.Exception (Exception)
import Control.Lens
import Control.Monad (replicateM, liftM2, liftM3, liftM4, liftM5, unless)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString.Char8 (ByteString)
import Data.ByteString.Lens (unpackedChars)
import Data.Digest.CRC32
import Data.Int
import Data.Serialize.Get
import Data.Serialize.Put
import GHC.Exts (IsString(..))
import System.IO
import Numeric.Lens
import Prelude hiding ((.), id)
import qualified Data.ByteString.Char8 as B
import qualified Network

doRequest' :: (RequestMessage req, Deserializable resp, MonadIO m) => CorrelationId -> Handle -> Request req -> m (Either String resp)
doRequest' correlationId h r = do
  rawLength <- liftIO $ do
    B.hPut h $ requestBytes r
    hFlush h
    B.hGet h 4
  case runGet (fmap fromIntegral getWord32be) rawLength of
    Left s -> return $ Left s
    Right dataLength -> do
      responseBytes <- liftIO $ B.hGet h dataLength
      return $ flip runGet responseBytes $ do
        correlationId' <- deserialize
        unless (correlationId == correlationId') $ fail ("Expected " ++ show correlationId ++ " but got " ++ show correlationId')
        isolate (dataLength - 4) deserialize

doRequest :: (RequestMessage req, Deserializable resp, MonadIO m) => ClientId -> CorrelationId -> Handle -> req -> m (Either String resp)
doRequest clientId correlationId h req = doRequest' correlationId h $ Request (correlationId, clientId, req)

class Serializable a where
  serialize :: a -> Put

class Deserializable a where
  deserialize :: Get a

class RequestMessage r where
  apiKeyValue :: r -> Int16
  apiVersionValue :: r -> Int16
  serializeRequest :: r -> Put

newtype GroupCoordinatorResponseV0 = GroupCoordinatorRespV0 (KafkaError, Broker) deriving (Show, Eq, Deserializable)

newtype ApiKey = ApiKey Int16 deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum) -- numeric ID for API (i.e. metadata req, produce req, etc.)
newtype ApiVersion = ApiVersion Int16 deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum)
newtype CorrelationId = CorrelationId Int32 deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum)
newtype ClientId = ClientId KafkaString deriving (Show, Eq, Deserializable, Serializable, IsString)

data MetadataRequest req resp where
  MetadataRequestV0 :: MetadataRequestV0 -> MetadataRequest MetadataRequestV0 MetadataResponseV0

newtype MetadataRequestV0 = MetadataReqV0 [TopicName] deriving (Show, Eq, Serializable, Deserializable)
newtype TopicName = TName { _tName :: KafkaString } deriving (Eq, Ord, Deserializable, Serializable, IsString)

instance Show TopicName where
  show = show . B.unpack . _kString. _tName

newtype KafkaBytes = KBytes { _kafkaByteString :: ByteString } deriving (Show, Eq, IsString)
newtype KafkaString = KString { _kString :: ByteString } deriving (Show, Eq, Ord, IsString)
newtype KafkaNullableString = KNString { _KNString :: Maybe ByteString } deriving (Show, Eq, Ord)

newtype ProduceResponseV0 =
  ProduceRespV0 { _produceResponseFieldsV0 :: [(TopicName, [(Partition, KafkaError, Offset)])] }
  deriving (Show, Eq, Deserializable, Serializable)

newtype ProduceResponseV1 =
  ProduceRespV1 { _produceResponseFieldsV1 :: ([(TopicName, [(Partition, KafkaError, Offset)])], ThrottleTimeMs) }
  deriving (Show, Eq, Deserializable, Serializable)

newtype ProduceResponseV2 =
  ProduceRespV2 { _produceResponseFieldsV2 :: ([(TopicName, [(Partition, KafkaError, Offset, Time)])], ThrottleTimeMs) }
  deriving (Show, Eq, Deserializable, Serializable)

newtype OffsetResponseV0 =
  OffsetRespV0 { _offsetResponseFieldsV0 :: [(TopicName, [PartitionOffsetsV0])] }
  deriving (Show, Eq, Deserializable)

newtype PartitionOffsetsV0 =
  PartitionOffsetsV0 { _partitionOffsetsFieldsV0 :: (Partition, KafkaError, [Offset]) }
  deriving (Show, Eq, Deserializable)

newtype OffsetResponseV1 =
  OffsetRespV1 { _offsetResponseFieldsV1 :: [(TopicName, [PartitionOffsetsV1])] }
  deriving (Show, Eq, Deserializable)

newtype PartitionOffsetsV1 =
  PartitionOffsetsV1 { _partitionOffsetsFieldsV1 :: (Partition, KafkaError, Time, Offset) }
  deriving (Show, Eq, Deserializable)

newtype FetchResponseV0 =
  FetchRespV0 { _fetchResponseFieldsV0 :: [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])] }
  deriving (Show, Eq, Serializable, Deserializable)

newtype FetchResponseV1 =
  FetchRespV1 { _fetchResponseFieldsV1 :: (ThrottleTimeMs, [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])]) }
  deriving (Show, Eq, Serializable, Deserializable)

newtype FetchResponseV2 =
  FetchRespV2 { _fetchResponseFieldsV2 :: (ThrottleTimeMs, [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])]) }
  deriving (Show, Eq, Serializable, Deserializable)

newtype FetchResponseV3 =
  FetchRespV3 { _fetchResponseFieldsV3 :: (ThrottleTimeMs, [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])]) }
  deriving (Show, Eq, Serializable, Deserializable)

newtype MetadataResponseV0 = MetadataRespV0 { _metadataResponseFieldsV0 :: ([Broker], [TopicMetadata]) } deriving (Show, Eq, Deserializable)
newtype Broker = Broker { _brokerFields :: (NodeId, Host, Port) } deriving (Show, Eq, Ord, Deserializable)
newtype NodeId = NodeId { _nodeId :: Int32 } deriving (Show, Eq, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Host = Host { _hostKString :: KafkaString } deriving (Show, Eq, Ord, Deserializable, IsString)
newtype Port = Port { _portInt :: Int32 } deriving (Show, Eq, Deserializable, Num, Integral, Ord, Real, Enum)
newtype ThrottleTimeMs = ThrottleTimeMs { _throttleTimeMs :: Int32 } deriving (Show, Eq, Deserializable, Num, Serializable, Integral, Ord, Real, Enum)
newtype TopicMetadata = TopicMetadata { _topicMetadataFields :: (KafkaError, TopicName, [PartitionMetadata]) } deriving (Show, Eq, Deserializable)
newtype PartitionMetadata = PartitionMetadata { _partitionMetadataFields :: (KafkaError, Partition, Leader, Replicas, Isr) } deriving (Show, Eq, Deserializable)
newtype Leader = Leader { _leaderId :: Maybe Int32 } deriving (Show, Eq, Ord)

newtype Replicas = Replicas [Int32] deriving (Show, Eq, Serializable, Deserializable)
newtype Isr = Isr [Int32] deriving (Show, Eq, Deserializable)

newtype OffsetCommitResponseV0 = OffsetCommitRespV0 [(TopicName, [(Partition, KafkaError)])] deriving (Show, Eq, Deserializable)
newtype OffsetCommitResponseV1 = OffsetCommitRespV1 [(TopicName, [(Partition, KafkaError)])] deriving (Show, Eq, Deserializable)
newtype OffsetCommitResponseV2 = OffsetCommitRespV2 [(TopicName, [(Partition, KafkaError)])] deriving (Show, Eq, Deserializable)

newtype OffsetFetchResponseV0 = OffsetFetchRespV0 [(TopicName, [(Partition, Offset, Metadata, KafkaError)])] deriving (Show, Eq, Deserializable)
newtype OffsetFetchResponseV1 = OffsetFetchRespV1 [(TopicName, [(Partition, Offset, Metadata, KafkaError)])] deriving (Show, Eq, Deserializable)

data OffsetRequest req resp where
  OffsetRequestV0 :: OffsetRequestV0 -> OffsetRequest OffsetRequestV0 OffsetResponseV0
  OffsetRequestV1 :: OffsetRequestV1 -> OffsetRequest OffsetRequestV1 OffsetResponseV1

newtype OffsetRequestV0 = OffsetReqV0 (ReplicaId, [(TopicName, [(Partition, Time, MaxNumberOfOffsets)])]) deriving (Show, Eq, Serializable)
newtype OffsetRequestV1 = OffsetReqV1 (ReplicaId, [(TopicName, [(Partition, Time, MaxNumberOfOffsets)])]) deriving (Show, Eq, Serializable)

newtype Time = Time { _timeInt :: Int64 } deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum, Bounded)
newtype MaxNumberOfOffsets = MaxNumberOfOffsets Int32 deriving (Show, Eq, Serializable, Num, Integral, Ord, Real, Enum)

data FetchRequest req resp where
  FetchRequestV0 :: FetchRequestV0 -> FetchRequest FetchRequestV0 FetchResponseV0
  FetchRequestV1 :: FetchRequestV1 -> FetchRequest FetchRequestV1 FetchResponseV1
  FetchRequestV2 :: FetchRequestV2 -> FetchRequest FetchRequestV2 FetchResponseV2
  FetchRequestV3 :: FetchRequestV3 -> FetchRequest FetchRequestV3 FetchResponseV3

newtype FetchRequestV0 =
  FetchReqV0 (ReplicaId, MaxWaitTime, MinBytes,
            [(TopicName, [(Partition, Offset, MaxBytes)])])
  deriving (Show, Eq, Deserializable, Serializable)

newtype FetchRequestV1 =
  FetchReqV1 (ReplicaId, MaxWaitTime, MinBytes,
              [(TopicName, [(Partition, Offset, MaxBytes)])])
    deriving (Show, Eq, Deserializable, Serializable)

newtype FetchRequestV2 =
  FetchReqV2 (ReplicaId, MaxWaitTime, MinBytes,
              [(TopicName, [(Partition, Offset, MaxBytes)])])
    deriving (Show, Eq, Deserializable, Serializable)

newtype FetchRequestV3 =
  FetchReqV3 (ReplicaId, MaxWaitTime, MinBytes, MaxBytes,
              [(TopicName, [(Partition, Offset, MaxBytes)])])
    deriving (Show, Eq, Deserializable, Serializable)

newtype ReplicaId = ReplicaId Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)
newtype MaxWaitTime = MaxWaitTime Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)
newtype MinBytes = MinBytes Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)
newtype MaxBytes = MaxBytes Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)

data ProduceRequest req resp where
  ProduceRequestV0 :: ProduceRequestV0 -> ProduceRequest ProduceRequestV0 ProduceResponseV0
  ProduceRequestV1 :: ProduceRequestV1 -> ProduceRequest ProduceRequestV1 ProduceResponseV1
  ProduceRequestV2 :: ProduceRequestV2 -> ProduceRequest ProduceRequestV2 ProduceResponseV2

newtype ProduceRequestV0 =
  ProduceReqV0 (RequiredAcks, Timeout,
              [(TopicName, [(Partition, MessageSet)])])
  deriving (Show, Eq, Serializable)

newtype ProduceRequestV1 =
  ProduceReqV1 (RequiredAcks, Timeout,
              [(TopicName, [(Partition, MessageSet)])])
  deriving (Show, Eq, Serializable)

newtype ProduceRequestV2 =
  ProduceReqV2 (RequiredAcks, Timeout,
              [(TopicName, [(Partition, MessageSet)])])
  deriving (Show, Eq, Serializable)

newtype RequiredAcks =
  RequiredAcks Int16 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Timeout =
  Timeout Int32 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Partition =
  Partition Int32 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)

newtype MessageSet =
  MessageSet { _messageSetMembers :: [MessageSetMember] } deriving (Show, Eq)
data MessageSetMember =
  MessageSetMember { _setOffset :: Offset, _setMessage :: Message } deriving (Show, Eq)
            
newtype Offset = Offset Int64 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)

data Message = MessageV0 { _messageFieldsV0 :: (Crc, MagicByte, Attributes, Key, Value) }
             | MessageV1 { _messageFieldsV1 :: (Crc, MagicByte, Attributes, Time, Key, Value) }
  deriving (Show, Eq)

newtype Crc = Crc Int32 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype MagicByte = MagicByte Int8 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Attributes = Attributes Int8 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)

newtype Key = Key { _keyBytes :: Maybe KafkaBytes } deriving (Show, Eq)
newtype Value = Value { _valueBytes :: Maybe KafkaBytes } deriving (Show, Eq)

data GroupCoordinatorRequest req resp where
  GroupCoordinatorRequestV0 :: GroupCoordinatorRequestV0 -> GroupCoordinatorRequest GroupCoordinatorRequestV0 GroupCoordinatorResponseV0

newtype GroupCoordinatorRequestV0 = GroupCoordinatorReqV0 ConsumerGroup deriving (Show, Eq, Serializable)

data OffsetCommitRequest req resp where
  OffsetCommitRequestV0 :: OffsetCommitRequestV0 -> OffsetCommitRequest OffsetCommitRequestV0 OffsetCommitResponseV0
  OffsetCommitRequestV1 :: OffsetCommitRequestV1 -> OffsetCommitRequest OffsetCommitRequestV1 OffsetCommitResponseV1
  OffsetCommitRequestV2 :: OffsetCommitRequestV2 -> OffsetCommitRequest OffsetCommitRequestV2 OffsetCommitResponseV2

newtype OffsetCommitRequestV0 = OffsetCommitReqV0 (ConsumerGroup, [(TopicName, [(Partition, Offset, Time, Metadata)])]) deriving (Show, Eq, Serializable)

newtype OffsetCommitRequestV1 = OffsetCommitReqV1 (ConsumerGroup, GroupGenerationId, MemberId, [(TopicName, [(Partition, Offset, Time, Metadata)])]) deriving (Show, Eq, Serializable)

newtype OffsetCommitRequestV2 = OffsetCommitReqV2 (
    ConsumerGroup,
    GroupGenerationId,
    MemberId,
    RetentionTime,
    [(TopicName, [(Partition, Offset, Time, Metadata)])]
  ) deriving (Show, Eq, Serializable)

newtype GroupGenerationId = GroupGenerationId { _groupGenerationId :: Int32 } deriving (Show, Eq, Serializable, Deserializable)
newtype MemberId = MemberId { _memberId :: KafkaString } deriving (Show, Eq, Serializable, Deserializable, IsString)
type RetentionTime = Time

data OffsetFetchRequest req resp where
  OffsetFetchRequestV0 :: OffsetFetchRequestV0 -> OffsetFetchRequest OffsetFetchRequestV0 OffsetFetchResponseV0
  OffsetFetchRequestV1 :: OffsetFetchRequestV1 -> OffsetFetchRequest OffsetFetchRequestV1 OffsetFetchResponseV1

newtype OffsetFetchRequestV0 = OffsetFetchReqV0 (ConsumerGroup, [(TopicName, [Partition])]) deriving (Show, Eq, Serializable)
newtype OffsetFetchRequestV1 = OffsetFetchReqV1 (ConsumerGroup, [(TopicName, [Partition])]) deriving (Show, Eq, Serializable)

newtype ConsumerGroup = ConsumerGroup KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)
newtype Metadata = Metadata KafkaNullableString deriving (Show, Eq, Serializable, Deserializable, IsString)

data JoinGroupRequest req resp where
  JoinGroupRequestV0 :: JoinGroupRequestV0 -> JoinGroupRequest JoinGroupRequestV0 JoinGroupResponseV0
  JoinGroupRequestV1 :: JoinGroupRequestV1 -> JoinGroupRequest JoinGroupRequestV1 JoinGroupResponseV1

newtype JoinGroupRequestV0 = JoinGroupReqV0 (ConsumerGroup, SessionTimeout, MemberId, ProtocolType, [(ProtocolName, ProtocolMetadata)]) deriving (Show, Eq, Serializable)
newtype JoinGroupRequestV1 = JoinGroupReqV1 (ConsumerGroup, SessionTimeout, RebalanceTimeout, MemberId, ProtocolType, [(ProtocolName, ProtocolMetadata)]) deriving (Show, Eq, Serializable)

newtype JoinGroupResponseV0 = JoinGroupResponseV0 { _joinGroupResponseFieldsV0 :: (KafkaError, GroupGenerationId, ProtocolName, GroupLeader, MemberId, [(MemberId, MemberMetadata)]) }
  deriving (Show, Eq, Deserializable)
newtype JoinGroupResponseV1 = JoinGroupResponseV1 { _joinGroupResponseFieldsV1 :: (KafkaError, GroupGenerationId, ProtocolName, GroupLeader, MemberId, [(MemberId, ProtocolMetadata)]) }
  deriving (Show, Eq, Deserializable)

type SessionTimeout = Timeout;
type RebalanceTimeout = Timeout;
newtype GroupLeader = GroupLeader KafkaString deriving (Show, Eq, Deserializable, IsString)
newtype ProtocolType = ProtocolType KafkaString deriving (Show, Eq, Serializable, IsString)
newtype ProtocolName = ProtocolName KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)
newtype ProtocolMetadata = ProtocolMetadata { _protocolMetadata :: (ApiVersion, [TopicName], UserData) } deriving (Show, Eq, Serializable, Deserializable)
newtype MemberMetadata = MemberMetadata KafkaBytes deriving (Show, Eq, Deserializable)
newtype MemberAssignment = MemberAssignment (ApiVersion, [(TopicName, [Partition])], UserData) deriving (Show, Eq, Serializable, Deserializable)
newtype UserData = UserData KafkaBytes deriving (Show, Eq, Serializable, Deserializable)

data HeartbeatRequest req resp where
  HeartbeatRequestV0 :: HeartbeatRequestV0 -> HeartbeatRequest HeartbeatRequestV0 HeartbeatResponseV0

newtype HeartbeatRequestV0 = HeartbeatReqV0 (ConsumerGroup, GroupGenerationId, MemberId) deriving (Show, Eq, Serializable)
newtype HeartbeatResponseV0 = HeartbeatRespV0 KafkaError deriving (Show, Eq, Deserializable)

data LeaveGroupRequest req resp where
  LeaveGroupRequestV0 :: LeaveGroupRequestV0 -> LeaveGroupRequest LeaveGroupRequestV0 LeaveGroupResponseV0

newtype LeaveGroupRequestV0 = LeaveGroupReqV0 (ConsumerGroup, MemberId) deriving (Show, Eq, Serializable)
newtype LeaveGroupResponseV0 = LeaveGroupRespV0 KafkaError deriving (Show, Eq, Deserializable)

data SyncGroupRequest req resp where
  SyncGroupRequestV0 :: SyncGroupRequestV0 -> SyncGroupRequest SyncGroupRequestV0 SyncGroupResponseV0

newtype SyncGroupRequestV0 = SyncGroupReqV0 (ConsumerGroup, GroupGenerationId, MemberId, [(MemberId, MemberAssignment)]) deriving (Show, Eq, Serializable)
newtype SyncGroupResponseV0 = SyncGroupRespV0 (KafkaError, KafkaBytes) deriving (Show, Eq, Deserializable)

errorKafka :: KafkaError -> Int16
errorKafka NoError                             = 0
errorKafka Unknown                             = -1
errorKafka OffsetOutOfRange                    = 1
errorKafka InvalidMessage                      = 2
errorKafka UnknownTopicOrPartition             = 3
errorKafka InvalidMessageSize                  = 4
errorKafka LeaderNotAvailable                  = 5
errorKafka NotLeaderForPartition               = 6
errorKafka RequestTimedOut                     = 7
errorKafka BrokerNotAvailable                  = 8
errorKafka ReplicaNotAvailable                 = 9
errorKafka MessageSizeTooLarge                 = 10
errorKafka StaleControllerEpochCode            = 11
errorKafka OffsetMetadataTooLargeCode          = 12
errorKafka OffsetsLoadInProgressCode           = 14
errorKafka ConsumerCoordinatorNotAvailableCode = 15
errorKafka NotCoordinatorForConsumerCode       = 16
errorKafka InvalidTopicException               = 17
errorKafka RecordListTooLarge                  = 18
errorKafka NotEnoughReplicas                   = 19
errorKafka NotEnoughReplicasAfterAppend        = 20
errorKafka InvalidRequiredAcks                 = 21
errorKafka IllegalGeneration                   = 22
errorKafka InconsistentGroupProtocol           = 23
errorKafka InvalidGroupId                      = 24
errorKafka UnknownMemberId                     = 25
errorKafka InvalidSessionTimeout               = 26
errorKafka RebalanceInProgress                 = 27

data KafkaError = NoError -- ^ @0@ No error--it worked!
                | Unknown -- ^ @-1@ An unexpected server error
                | OffsetOutOfRange -- ^ @1@ The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
                | InvalidMessage -- ^ @2@ This indicates that a message contents does not match its CRC
                | UnknownTopicOrPartition -- ^ @3@ This request is for a topic or partition that does not exist on this broker.
                | InvalidMessageSize -- ^ @4@ The message has a negative size
                | LeaderNotAvailable -- ^ @5@ This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
                | NotLeaderForPartition -- ^ @6@ This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
                | RequestTimedOut -- ^ @7@ This error is thrown if the request exceeds the user-specified time limit in the request.
                | BrokerNotAvailable -- ^ @8@ This is not a client facing error and is used mostly by tools when a broker is not alive.
                | ReplicaNotAvailable -- ^ @9@ If replica is expected on a broker, but is not.
                | MessageSizeTooLarge -- ^ @10@ The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
                | StaleControllerEpochCode -- ^ @11@ Internal error code for broker-to-broker communication.
                | OffsetMetadataTooLargeCode -- ^ @12@ If you specify a string larger than configured maximum for offset metadata
                | OffsetsLoadInProgressCode -- ^ @14@ The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
                | ConsumerCoordinatorNotAvailableCode -- ^ @15@ The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
                | NotCoordinatorForConsumerCode -- ^ @16@ The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
                | InvalidTopicException
                | RecordListTooLarge
                | NotEnoughReplicas
                | NotEnoughReplicasAfterAppend
                | InvalidRequiredAcks
                | IllegalGeneration
                | InconsistentGroupProtocol
                | InvalidGroupId
                | UnknownMemberId
                | InvalidSessionTimeout
                | RebalanceInProgress
                deriving (Eq, Show)

instance IsString KafkaNullableString where
  fromString x = KNString (Just (fromString x))

instance Serializable KafkaError where
  serialize = serialize . errorKafka

instance Deserializable KafkaError where
  deserialize = do
    x <- deserialize :: Get Int16
    case x of
      0    -> return NoError
      (-1) -> return Unknown
      1    -> return OffsetOutOfRange
      2    -> return InvalidMessage
      3    -> return UnknownTopicOrPartition
      4    -> return InvalidMessageSize
      5    -> return LeaderNotAvailable
      6    -> return NotLeaderForPartition
      7    -> return RequestTimedOut
      8    -> return BrokerNotAvailable
      9    -> return ReplicaNotAvailable
      10   -> return MessageSizeTooLarge
      11   -> return StaleControllerEpochCode
      12   -> return OffsetMetadataTooLargeCode
      14   -> return OffsetsLoadInProgressCode
      15   -> return ConsumerCoordinatorNotAvailableCode
      16   -> return NotCoordinatorForConsumerCode
      17   -> return InvalidTopicException
      18   -> return RecordListTooLarge
      19   -> return NotEnoughReplicas
      20   -> return NotEnoughReplicasAfterAppend
      21   -> return InvalidRequiredAcks
      22   -> return IllegalGeneration
      23   -> return InconsistentGroupProtocol
      24   -> return InvalidGroupId
      25   -> return UnknownMemberId
      26   -> return InvalidSessionTimeout
      27   -> return RebalanceInProgress
      _    -> fail $ "invalid error code: " ++ show x

instance Exception KafkaError

newtype Request req = Request (CorrelationId, ClientId, req)

instance RequestMessage req => Serializable (Request req) where
  serialize (Request (correlationId, clientId, r)) = do
    serialize (apiKey r)
    serialize (apiVersion r)
    serialize correlationId
    serialize clientId
    serializeRequest r

requestBytes :: RequestMessage req => Request req -> ByteString
requestBytes x = runPut $ do
  putWord32be . fromIntegral $ B.length mr
  putByteString mr
    where mr = runPut $ serialize x

apiVersion :: RequestMessage req => req -> ApiVersion
apiVersion = ApiVersion . apiVersionValue

apiKey :: RequestMessage req => req -> ApiKey
apiKey = ApiKey . apiKeyValue

instance Serializable Int64 where serialize = putWord64be . fromIntegral
instance Serializable Int32 where serialize = putWord32be . fromIntegral
instance Serializable Int16 where serialize = putWord16be . fromIntegral
instance Serializable Int8  where serialize = putWord8    . fromIntegral

instance Serializable Bool where
  serialize True = serialize (1 :: Int8)
  serialize False = serialize (0 :: Int8)

instance Serializable Key where
  serialize (Key (Just bs)) = serialize bs
  serialize (Key Nothing)   = serialize (-1 :: Int32)

instance Serializable Value where
  serialize (Value (Just bs)) = serialize bs
  serialize (Value Nothing)   = serialize (-1 :: Int32)

instance Serializable KafkaString where
  serialize (KString bs) = do
    let l = fromIntegral (B.length bs) :: Int16
    serialize l
    putByteString bs

instance Serializable KafkaNullableString where
  serialize (KNString (Just bs)) = serialize (KString bs)
  serialize (KNString Nothing) = serialize (-1 :: Int16)

instance Serializable MessageSet where
  serialize (MessageSet ms) = do
    let bytes = runPut $ mapM_ serialize ms
        l = fromIntegral (B.length bytes) :: Int32
    serialize l
    putByteString bytes

instance Serializable KafkaBytes where
  serialize (KBytes bs) = do
    let l = fromIntegral (B.length bs) :: Int32
    serialize l
    putByteString bs

instance Serializable MessageSetMember where
  serialize (MessageSetMember offset msg) = do
    serialize offset
    serialize msize
    serialize msg
      where msize = fromIntegral $ B.length $ runPut $ serialize msg :: Int32

instance Serializable Message where
  serialize (MessageV0 (_, magic, attrs, k, v)) = do
    let m = runPut $ serialize magic >> serialize attrs >> serialize k >> serialize v
    putWord32be (crc32 m)
    putByteString m

  serialize (MessageV1 (_, magic, attrs, t, k, v)) = do
    let m = runPut $ serialize magic >> serialize attrs >> serialize t >> serialize k >> serialize v
    putWord32be (crc32 m)
    putByteString m

instance (Serializable a) => Serializable [a] where
  serialize xs = do
    let l = fromIntegral (length xs) :: Int32
    serialize l
    mapM_ serialize xs

instance (Serializable a, Serializable b) => Serializable ((,) a b) where
  serialize (x, y) = serialize x >> serialize y
instance (Serializable a, Serializable b, Serializable c) => Serializable ((,,) a b c) where
  serialize (x, y, z) = serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d) => Serializable ((,,,) a b c d) where
  serialize (w, x, y, z) = serialize w >> serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d, Serializable e) => Serializable ((,,,,) a b c d e) where
  serialize (v, w, x, y, z) = serialize v >> serialize w >> serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d, Serializable e, Serializable f) => Serializable ((,,,,,) a b c d e f) where
  serialize (u, v, w, x, y, z) = serialize u >> serialize v >> serialize w >> serialize x >> serialize y >> serialize z

instance Deserializable MessageSet where
  deserialize = do
    l <- deserialize :: Get Int32
    ms <- isolate (fromIntegral l) getMembers
    return $ MessageSet ms
      where getMembers :: Get [MessageSetMember]
            getMembers = do
              wasEmpty <- isEmpty
              if wasEmpty
              then return []
              else liftM2 (:) deserialize getMembers <|> (remaining >>= getBytes >> return [])

instance Deserializable MessageSetMember where
  deserialize = do
    o <- deserialize
    l <- deserialize :: Get Int32
    m <- isolate (fromIntegral l) deserialize
    return $ MessageSetMember o m

instance Deserializable Message where
  deserialize = do
    (crc, magicByte, attributes) <- deserialize :: Get (Crc, MagicByte, Attributes)

    case magicByte of
      0 -> do
        (key, value) <- deserialize :: Get (Key, Value)
        return $ MessageV0 (crc, magicByte, attributes, key, value)

      1 -> do
        (time, key, value) <- deserialize :: Get (Time, Key, Value)
        return $ MessageV1 (crc, magicByte, attributes, time, key, value)

      _ -> fail "Unsupported magic byte value"

instance Deserializable Leader where
  deserialize = do
    x <- deserialize :: Get Int32
    let l = Leader $ if x == -1 then Nothing else Just x
    return l

instance Deserializable KafkaBytes where
  deserialize = do
    l <- deserialize :: Get Int32
    bs <- getByteString $ fromIntegral l
    return $ KBytes bs

instance Deserializable KafkaString where
  deserialize = do
    l <- deserialize :: Get Int16
    bs <- getByteString $ fromIntegral l
    return $ KString bs

instance Deserializable KafkaNullableString where
  deserialize = do
    l <- deserialize :: Get Int16
    case l of
      -1 -> return $ KNString Nothing
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ KNString (Just bs)

instance Deserializable Bool where
  deserialize = do
    v <- deserialize :: Get Int8
    case v of
      0 -> return False
      1 -> return True
      _ -> fail $ "Unexpected Bool value " ++ show v

instance Deserializable Key where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return (Key Nothing)
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ Key (Just (KBytes bs))

instance Deserializable Value where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return (Value Nothing)
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ Value (Just (KBytes bs))

instance (Deserializable a) => Deserializable [a] where
  deserialize = do
    l <- deserialize :: Get Int32
    replicateM (fromIntegral l) deserialize

instance (Deserializable a, Deserializable b) => Deserializable ((,) a b) where
  deserialize = liftM2 (,) deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c) => Deserializable ((,,) a b c) where
  deserialize = liftM3 (,,) deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d) => Deserializable ((,,,) a b c d) where
  deserialize = liftM4 (,,,) deserialize deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d, Deserializable e) => Deserializable ((,,,,) a b c d e) where
  deserialize = liftM5 (,,,,) deserialize deserialize deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d, Deserializable e, Deserializable f) => Deserializable ((,,,,,) a b c d e f) where
  deserialize = liftM6 (,,,,,) deserialize deserialize deserialize deserialize deserialize deserialize
    where
      liftM6 f m1 m2 m3 m4 m5 m6 = do { x1 <- m1; x2 <- m2; x3 <- m3; x4 <- m4; x5 <- m5; x6 <- m6; return (f x1 x2 x3 x4 x5 x6) }


instance Deserializable Int64 where deserialize = fmap fromIntegral getWord64be
instance Deserializable Int32 where deserialize = fmap fromIntegral getWord32be
instance Deserializable Int16 where deserialize = fmap fromIntegral getWord16be
instance Deserializable Int8  where deserialize = fmap fromIntegral getWord8

instance Serializable req => RequestMessage (MetadataRequest req resp) where
  apiKeyValue _ = 3

  serializeRequest (MetadataRequestV0 r) = serialize r

  apiVersionValue MetadataRequestV0{} = 0

instance Serializable req => RequestMessage (ProduceRequest req resp) where
  apiKeyValue _ = 0

  serializeRequest (ProduceRequestV0 r) = serialize r
  serializeRequest (ProduceRequestV1 r) = serialize r
  serializeRequest (ProduceRequestV2 r) = serialize r

  apiVersionValue ProduceRequestV0{} = 0
  apiVersionValue ProduceRequestV1{} = 1
  apiVersionValue ProduceRequestV2{} = 2

instance Serializable req => RequestMessage (FetchRequest req resp) where
  apiKeyValue _ = 1

  serializeRequest (FetchRequestV0 r) = serialize r
  serializeRequest (FetchRequestV1 r) = serialize r
  serializeRequest (FetchRequestV2 r) = serialize r
  serializeRequest (FetchRequestV3 r) = serialize r

  apiVersionValue FetchRequestV0{} = 0
  apiVersionValue FetchRequestV1{} = 1
  apiVersionValue FetchRequestV2{} = 2
  apiVersionValue FetchRequestV3{} = 3

instance Serializable req => RequestMessage (OffsetRequest req resp) where
  apiKeyValue _ = 2

  serializeRequest (OffsetRequestV0 r) = serialize r
  serializeRequest (OffsetRequestV1 r) = serialize r

  apiVersionValue OffsetRequestV0{} = 0
  apiVersionValue OffsetRequestV1{} = 1

instance Serializable req => RequestMessage (OffsetCommitRequest req resp) where
  apiKeyValue _ = 8

  serializeRequest (OffsetCommitRequestV0 r) = serialize r
  serializeRequest (OffsetCommitRequestV1 r) = serialize r
  serializeRequest (OffsetCommitRequestV2 r) = serialize r

  apiVersionValue OffsetCommitRequestV0{} = 0
  apiVersionValue OffsetCommitRequestV1{} = 1
  apiVersionValue OffsetCommitRequestV2{} = 2

instance Serializable req => RequestMessage (OffsetFetchRequest req resp) where
  apiKeyValue _ = 2

  serializeRequest (OffsetFetchRequestV0 r) = serialize r
  serializeRequest (OffsetFetchRequestV1 r) = serialize r

  apiVersionValue OffsetFetchRequestV0{} = 0
  apiVersionValue OffsetFetchRequestV1{} = 1

instance Serializable req => RequestMessage (GroupCoordinatorRequest req resp) where
  apiKeyValue _ = 10

  serializeRequest (GroupCoordinatorRequestV0 r) = serialize r

  apiVersionValue GroupCoordinatorRequestV0{} = 0

instance Serializable req => RequestMessage (JoinGroupRequest req resp) where
  apiKeyValue _ = 11

  serializeRequest (JoinGroupRequestV0 r) = serialize r
  serializeRequest (JoinGroupRequestV1 r) = serialize r

  apiVersionValue JoinGroupRequestV0{} = 0
  apiVersionValue JoinGroupRequestV1{} = 1

instance Serializable req => RequestMessage (HeartbeatRequest req resp) where
  apiKeyValue _ = 12

  serializeRequest (HeartbeatRequestV0 r) = serialize r

  apiVersionValue HeartbeatRequestV0{} = 0

instance Serializable req => RequestMessage (LeaveGroupRequest req resp) where
  apiKeyValue _ = 13

  serializeRequest (LeaveGroupRequestV0 r) = serialize r

  apiVersionValue LeaveGroupRequestV0{} = 0

instance Serializable req => RequestMessage (SyncGroupRequest req resp) where
  apiKeyValue _ = 14

  serializeRequest (SyncGroupRequestV0 r) = serialize r

  apiVersionValue SyncGroupRequestV0{} = 0

-- * Generated lenses

makeLenses ''TopicName

makeLenses ''KafkaBytes
makeLenses ''KafkaString

makeLenses ''ProduceResponseV0
makeLenses ''ProduceResponseV1
makeLenses ''ProduceResponseV2

makeLenses ''OffsetResponseV0
makeLenses ''OffsetResponseV1
makeLenses ''PartitionOffsetsV0
makeLenses ''PartitionOffsetsV1

makeLenses ''FetchResponseV0
makeLenses ''FetchResponseV1
makeLenses ''FetchResponseV2
makeLenses ''FetchResponseV3

makeLenses ''MetadataResponseV0
makeLenses ''Broker
makeLenses ''NodeId
makeLenses ''Host
makeLenses ''Port
makeLenses ''TopicMetadata
makeLenses ''PartitionMetadata
makeLenses ''Leader

makeLenses ''Time

makeLenses ''Partition

makeLenses ''MessageSet
makeLenses ''MessageSetMember
makeLenses ''Offset

makeLenses ''Message

makeLenses ''Key
makeLenses ''Value

-- * Composed lenses

keyed :: (Field1 a a b b, Choice p, Applicative f, Eq b) => b -> Optic' p f a a
keyed k = filtered (view $ _1 . to (== k))

metadataResponseBrokers :: Lens' MetadataResponseV0 [Broker]
metadataResponseBrokers = metadataResponseFieldsV0 . _1

topicsMetadata :: Lens' MetadataResponseV0 [TopicMetadata]
topicsMetadata = metadataResponseFieldsV0 . _2

topicMetadataKafkaError :: Lens' TopicMetadata KafkaError
topicMetadataKafkaError = topicMetadataFields . _1

topicMetadataName :: Lens' TopicMetadata TopicName
topicMetadataName = topicMetadataFields . _2

partitionsMetadata :: Lens' TopicMetadata [PartitionMetadata]
partitionsMetadata = topicMetadataFields . _3

partitionId :: Lens' PartitionMetadata Partition
partitionId = partitionMetadataFields . _2

partitionMetadataLeader :: Lens' PartitionMetadata Leader
partitionMetadataLeader = partitionMetadataFields . _3

brokerNode :: Lens' Broker NodeId
brokerNode = brokerFields . _1

brokerHost :: Lens' Broker Host
brokerHost = brokerFields . _2

brokerPort :: Lens' Broker Port
brokerPort = brokerFields . _3

fetchResponseMessages :: Fold FetchResponseV1 MessageSet
fetchResponseMessages = fetchResponseFieldsV1 . _2 . folded . _2 . folded . _4

fetchResponseByTopic :: TopicName -> Fold FetchResponseV1 (Partition, KafkaError, Offset, MessageSet)
fetchResponseByTopic t = fetchResponseFieldsV1 . _2 . folded . keyed t . _2 . folded

messageSetByPartition :: Partition -> Fold (Partition, KafkaError, Offset, MessageSet) MessageSetMember
messageSetByPartition p = keyed p . _4 . messageSetMembers . folded

fetchResponseMessageMembers :: Fold FetchResponseV1 MessageSetMember
fetchResponseMessageMembers = fetchResponseMessages . messageSetMembers . folded

messageFields :: Lens' Message (Crc, MagicByte, Attributes, Key, Value)
messageFields = lens fields setter
  where fields :: Message -> (Crc, MagicByte, Attributes, Key, Value)
        fields m@MessageV0{} = _messageFieldsV0 m
        fields (MessageV1 (crc, magicByte, attributes, _, key, value)) = (crc, magicByte, attributes, key, value)

        setter :: Message -> (Crc, MagicByte, Attributes, Key, Value) -> Message
        setter MessageV0{} v = MessageV0 v
        setter (MessageV1 (_, _, _, time, _, _)) (crc, magicByte, attributes, key, value) = MessageV1 (crc, magicByte, attributes, time, key, value)

messageKey :: Lens' Message Key
messageKey = messageFields . _4

messageKeyBytes :: Fold Message ByteString
messageKeyBytes = messageKey . keyBytes . folded . kafkaByteString

messageValue :: Lens' Message Value
messageValue = messageFields . _5

payload :: Fold Message ByteString
payload = messageValue . valueBytes . folded . kafkaByteString

offsetResponseOffset :: Partition -> Fold OffsetResponseV1 Offset
offsetResponseOffset p = offsetResponseFieldsV1 . folded . _2 . folded . partitionOffsetsFieldsV1 . keyed p . _4

messageSet :: Partition -> TopicName -> Fold FetchResponseV1 MessageSetMember
messageSet p t = fetchResponseByTopic t . messageSetByPartition p

nextOffset :: Lens' MessageSetMember Offset
nextOffset = setOffset . adding 1

findPartitionMetadata :: Applicative f => TopicName -> LensLike' f TopicMetadata [PartitionMetadata]
findPartitionMetadata t = filtered (view $ topicMetadataName . to (== t)) . partitionsMetadata

findPartition :: Partition -> Prism' PartitionMetadata PartitionMetadata
findPartition p = filtered (view $ partitionId . to (== p))

hostString :: Lens' Host String
hostString = hostKString . kString . unpackedChars

portId :: IndexPreservingGetter Port Network.PortID
portId = portInt . to fromIntegral . to Network.PortNumber
