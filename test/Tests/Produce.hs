
module Tests.Produce where

import Control.Monad.Trans (liftIO)
import Data.Either (isRight)
import KafkaTest
import Network.Kafka.Producer
import Network.Kafka.Protocol
import Test.Tasty.Hspec

sendRequest :: (Serializable req, Deserializable resp, Show resp) => ProduceRequestFactory req resp -> [String] -> IO ()
sendRequest prf ms = do
  time <- liftIO currentTime
  result <- run $ produceMessages' prf $ byteMessages time ms
  result `shouldSatisfy` isRight

tests :: Spec
tests = do
  prop "v0" $ \ms -> do
    let prf = ProduceRequestFactory (\ts -> ProduceRequestV0 . ProduceReqV0 $ (1, 1000, formatTopicMessageSet ts))
    sendRequest prf ms

  prop "v1" $ \ms -> do
    let prf = ProduceRequestFactory (\ts -> ProduceRequestV1 . ProduceReqV1 $ (1, 1000, formatTopicMessageSet ts))
    sendRequest prf ms

  prop "v2" $ \ms -> do
    let prf = ProduceRequestFactory (\ts -> ProduceRequestV2 . ProduceReqV2 $ (1, 1000, formatTopicMessageSet ts))
    sendRequest prf ms

  prop "using Producer api" $ \ms -> do
    time <- liftIO currentTime
    result <- run . produceMessages $ byteMessages time ms
    result `shouldSatisfy` isRight

  prop "can produce multiple messages" $ \(ms, ms') -> do
    result <- run $ do
      time <- liftIO currentTime
      r1 <- produceMessages $ byteMessages time ms
      r2 <- produceMessages $ byteMessages time ms'
      return $ r1 ++ r2
    result `shouldSatisfy` isRight
