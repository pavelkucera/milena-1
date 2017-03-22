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
fetchRequest :: Kafka m => Offset -> Partition -> TopicName -> m (FetchRequest FetchRequestV1 FetchResponseV1)
fetchRequest o p topic = do
  wt <- use stateWaitTime
  ws <- use stateWaitSize
  bs <- use stateBufferSize
  return . FetchRequestV1 $ FetchReqV1 (ordinaryConsumerId, wt, ws, [(topic, [(p, o, bs)])])

-- | Execute a fetch request and get the raw fetch response.
fetch' :: Kafka m => Handle -> FetchRequest FetchRequestV1 FetchResponseV1 -> m FetchResponseV1
fetch' h request = makeRequest h $ FetchRR request

fetch :: Kafka m => Offset -> Partition -> TopicName -> m FetchResponseV1
fetch o p topic = do
  broker <- getTopicPartitionLeader topic p
  withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest o p topic)

-- | Extract out messages with their topics from a fetch response.
fetchMessages :: FetchResponseV1 -> [TopicAndMessage]
fetchMessages fr = (fr ^.. fetchResponseFieldsV1 . _2 . folded) >>= tam
    where tam a = TopicAndMessage (a ^. _1) <$> a ^.. _2 . folded . _4 . messageSetMembers . folded . setMessage
