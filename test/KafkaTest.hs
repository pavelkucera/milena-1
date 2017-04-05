{-# LANGUAGE OverloadedStrings #-}

module KafkaTest where

import Control.Lens
import Control.Monad.Except (ExceptT(..))
import Control.Monad.Trans.State(StateT(..))
import Network.Kafka.Producer
import Network.Kafka.Protocol
import Network.Kafka
import Test.Tasty.Hspec
import Test.Tasty.QuickCheck
import qualified Data.ByteString.Char8 as B

topic :: TopicName
topic = "milena-test"

byteMessages :: Functor f => Time -> f String -> f TopicAndMessage
byteMessages time = fmap (TopicAndMessage topic . makeMessage time . B.pack)

run :: StateT KafkaState (ExceptT KafkaClientError IO) a -> IO (Either KafkaClientError a)
run = runKafka $ mkKafkaState "milena-test-client" ("localhost", 9092)

requireAllAcks :: StateT KafkaState (ExceptT KafkaClientError IO) ()
requireAllAcks = do
  stateRequiredAcks .= -1
  stateWaitSize .= 1
  stateWaitTime .= 1000

prop :: Testable prop => String -> prop -> SpecWith ()
prop s = it s . property
