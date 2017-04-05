{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Functor
import Data.Either (isRight, isLeft)
import qualified Data.List.NonEmpty as NE
import Control.Lens
import Control.Monad.Except (catchError, throwError)
import Control.Monad.Trans (liftIO)
import Network.Kafka
import Network.Kafka.Consumer
import Network.Kafka.Producer
import Network.Kafka.Protocol (ProduceResponseV2(..), KafkaError(..))
import Test.Tasty
import Test.Tasty.Hspec
import Test.Tasty.QuickCheck
import qualified Data.ByteString.Char8 as B
import KafkaTest

import Prelude

main :: IO ()
main = testSpec "the specs" specs >>= defaultMain

specs :: Spec
specs = do

  describe "can talk to local Kafka server" $ do
    prop "can produce messages" $ \ms -> do
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

    prop "can fetch messages" $ do
      result <- run $ do
        offset <- getLastOffset EarliestTime 0 topic
        withAnyHandle (\handle -> fetch' handle =<< fetchRequest offset 0 topic)
      result `shouldSatisfy` isRight

    prop "can roundtrip messages" $ \ms key -> do
      time <- liftIO currentTime
      let messages = byteMessages time ms
      result <- run $ do
        requireAllAcks
        info <- brokerPartitionInfo topic

        case getPartitionByKey (B.pack key) info of
          Just PartitionAndLeader { _palLeader = leader, _palPartition = partition } -> do
            let payload = [(TopicAndPartition topic partition, groupMessagesToSet messages)]
                s = stateBrokers . at leader
            ([(_topicName, [(_, NoError, offset, _)])], _) <- _produceResponseFieldsV2 <$> send leader payload
            broker <- findMetadataOrElse [topic] s (KafkaInvalidBroker leader)
            resp <- withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest offset partition topic)
            return $ fmap tamPayload . fetchMessages $ resp

          Nothing -> fail "Could not deduce partition"

      result `shouldBe` Right (tamPayload <$> messages)

    prop "can roundtrip keyed messages" $ \(NonEmpty ms) key -> do
      time <- liftIO currentTime
      let keyBytes = B.pack key
          messages = fmap (TopicAndMessage topic . makeKeyedMessage time keyBytes . B.pack) ms
      result <- run $ do
        requireAllAcks
        produceResps <- produceMessages messages

        case map _produceResponseFieldsV2 produceResps of
          [([(_topicName, [(partition, NoError, offset, _)])], _)] -> do
            resp <- fetch offset partition topic
            return $ fmap tamPayload . fetchMessages $ resp

          _ -> fail "Unexpected produce response"

      result `shouldBe` Right (tamPayload <$> messages)

  describe "withAddressHandle" $ do
    it "turns 'IOException's into 'KafkaClientError's" $ do
      result <- run $ withAddressHandle ("localhost", 9092) (\_ -> liftIO $ ioError $ userError "SOMETHING WENT WRONG!") :: IO (Either KafkaClientError ())
      result `shouldSatisfy` isLeft

    it "discards monadic effects when exceptions are thrown" $ do
      result <- run $ do
        stateName .= "expected"
        _ <- flip catchError (return . Left) $ withAddressHandle ("localhost", 9092) $ \_ -> do
          stateName .= "changed"
          _ <- throwError KafkaFailedToFetchMetadata
          n <- use stateName
          return (Right n)
        use stateName
      result `shouldBe` Right "expected"

  describe "updateMetadatas" $
    it "de-dupes _stateAddresses" $ do
      result <- run $ do
        stateAddresses %= NE.cons ("localhost", 9092)
        updateMetadatas []
        use stateAddresses
      result `shouldBe` fmap NE.nub result
