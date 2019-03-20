package com.github.rerorero.reroft.raft

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import com.github.rerorero.reroft.log.LogRepository
import com.github.rerorero.reroft.test.TestUtil
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class RaftActorSpec
  extends TestKit(ActorSystem("test"))
  with ImplicitSender
  with Matchers
  with WordSpecLike
  with BeforeAndAfterAll
  with ScalaFutures
  with TestUtil
{
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "RaftServerImpl" should {
    "be initialized" in {
      val expected = sample[Configure]
      val mockedSM = TestProbe()
      val sut = TestFSMRef(new RaftActor(mockedSM.ref, sample[LogRepository]))
      sut ! Configure(expected.nodes, expected.me)
      assert(sut.stateName === Follower)
      assert(sut.stateData.clusterNodes === expected.nodes)
      assert(sut.stateData.myID === expected.me)
    }
  }
}
