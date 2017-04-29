
module Tests.Consume where

import Data.Either (isRight)
import KafkaTest
import Network.Kafka
import Network.Kafka.Consumer
import Network.Kafka.Protocol
import Test.Tasty.Hspec

sendRequest :: (Serializable req, Deserializable resp, Show resp) => (Offset -> FetchRequest req resp) -> IO ()
sendRequest r = do
  result <- run $ do
    offset <- getLastOffset EarliestTime 0 topic
    withAnyHandle (\handle -> fetch' handle $ r offset)
  result `shouldSatisfy` isRight

tests :: Spec
tests = do
  prop "v0" $ sendRequest $ \offset -> FetchRequestV0 $ FetchReqV0 (ordinaryConsumerId, 0, 0, [(topic, [(0, offset, 1024)])])

  prop "v1" $ sendRequest $ \offset -> FetchRequestV1 $ FetchReqV1 (ordinaryConsumerId, 0, 0, [(topic, [(0, offset, 1024)])])

  prop "v2" $ sendRequest $ \offset -> FetchRequestV2 $ FetchReqV2 (ordinaryConsumerId, 0, 0, [(topic, [(0, offset, 1024)])])

  prop "v3" $ sendRequest $ \offset -> FetchRequestV3 $ FetchReqV3 (ordinaryConsumerId, 0, 0, 1024 * 1024, [(topic, [(0, offset, 1024)])])

  prop "using Consumer api" $ do
    result <- run $ do
      offset <- getLastOffset EarliestTime 0 topic
      withAnyHandle (\handle -> fetch' handle =<< fetchRequest offset 0 topic)
    result `shouldSatisfy` isRight
