{-# LANGUAGE OverloadedStrings #-}

module Tests.Group where

import KafkaTest
import Network.Kafka
import Network.Kafka.Consumer
import Network.Kafka.Protocol
import Test.Tasty.Hspec
import qualified Data.ByteString.Char8 as B
import Data.IORef

groupName :: ConsumerGroup
groupName = "milena-test-group"

tests :: Spec
tests = do
  member <- runIO $ newIORef (MemberId "", GroupGenerationId 0)

  it "joins a group" $ do
    result <- run $
      withAnyHandle (\handle -> joinGroup handle =<< joinGroupRequest groupName "" "type" [
          ("name", ProtocolMetadata (ApiVersion 0, [topic], UserData . KBytes $ B.empty))
        ])

    case result of
      Right (JoinGroupResponseV0 (err, gid, name, GroupLeader leader, MemberId mid, _)) -> do
        err `shouldBe` NoError
        name `shouldBe` "name"
        mid `shouldNotBe` ""
        leader `shouldBe` mid

        writeIORef member (MemberId mid, gid)

      _ -> fail "Join group request failed"

  it "synces the group" $ do
    (mid, gid) <- readIORef member
    result <- run $
      withAnyHandle (\handle -> syncGroup handle $ SyncGroupRequestV0 (SyncGroupReqV0 (groupName, gid, mid, [])))

    case result of
      Right (SyncGroupRespV0 (err, _)) ->
        err `shouldBe` NoError

      _ -> fail "Sync group request failed"

  it "sends a heartbeat" $ do
    (mid, gid) <- readIORef member
    result <- run $
      withAnyHandle (\handle -> sendHeartbeat handle $ HeartbeatRequestV0 (HeartbeatReqV0 (groupName, gid, mid)))

    case result of
      Right (HeartbeatRespV0 err) ->
        err `shouldBe` NoError

      _ -> fail "Heartbeat request failed"

  it "leaves the group" $ do
    (mid, _) <- readIORef member
    result <- run $
      withAnyHandle (\handle -> leaveGroup handle $ LeaveGroupRequestV0 (LeaveGroupReqV0 (groupName, mid)))

    case result of
      Right (LeaveGroupRespV0 err) ->
        err `shouldBe` NoError

      _ -> fail "Leave group request failed"
