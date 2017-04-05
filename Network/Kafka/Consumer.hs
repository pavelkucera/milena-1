{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Network.Kafka.Consumer where

import Control.Applicative
import Control.Lens
import System.IO
import Prelude

import Network.Kafka
import Network.Kafka.Protocol

-- * Fetching

-- | Default: @-1@
ordinaryConsumerId :: ReplicaId
ordinaryConsumerId = ReplicaId (-1)

-- | Construct a fetch request from the values in the state.
fetchRequest :: Kafka m => Offset -> Partition -> TopicName -> m (FetchRequest FetchRequestV3 FetchResponseV3)
fetchRequest o p topic = do
  wt <- use stateWaitTime
  ws <- use stateWaitSize
  wm <- use stateWaitMaxSize
  bs <- use stateBufferSize
  return . FetchRequestV3 $ FetchReqV3 (ordinaryConsumerId, wt, ws, wm, [(topic, [(p, o, bs)])])

-- | Execute a fetch request and get the raw fetch response.
fetch' :: (Serializable req, Deserializable resp, Kafka m)  => Handle -> FetchRequest req resp -> m resp
fetch' = makeRequest

fetch :: Kafka m => Offset -> Partition -> TopicName -> m FetchResponseV3
fetch o p topic = do
  broker <- getTopicPartitionLeader topic p
  withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest o p topic)

-- | Extract out messages with their topics from a fetch response.
fetchMessages :: FetchResponseV3 -> [TopicAndMessage]
fetchMessages fr = (fr ^.. fetchResponseFieldsV3 . _2 . folded) >>= tam
    where tam a = TopicAndMessage (a ^. _1) <$> a ^.. _2 . folded . _4 . messageSetMembers . folded . setMessage

-- | Construct a join group request from the values in the state
joinGroupRequest :: Kafka m => ConsumerGroup -> MemberId -> ProtocolType -> [(ProtocolName, ProtocolMetadata)] -> m (JoinGroupRequest JoinGroupRequestV0 JoinGroupResponseV0)
joinGroupRequest cg mi pt gp = do
  st <- use stateGroupSessionTimeout
  return . JoinGroupRequestV0 $ JoinGroupReqV0  (cg, st, mi, pt, gp)

-- Send the given join group request
joinGroup :: (Serializable req, Deserializable resp, Kafka m) => Handle -> JoinGroupRequest req resp -> m resp
joinGroup = makeRequest

-- Send the given heartbeat request
sendHeartbeat :: (Serializable req, Deserializable resp, Kafka m) => Handle -> HeartbeatRequest req resp -> m resp
sendHeartbeat = makeRequest

-- Send the given sync group request
syncGroup :: (Serializable req, Deserializable resp, Kafka m) => Handle -> SyncGroupRequest req resp -> m resp
syncGroup = makeRequest

-- Send the given sync group request
leaveGroup :: (Serializable req, Deserializable resp, Kafka m) => Handle -> LeaveGroupRequest req resp -> m resp
leaveGroup = makeRequest
