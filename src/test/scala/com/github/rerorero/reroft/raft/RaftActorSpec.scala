package com.github.rerorero.reroft.raft

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import com.github.rerorero.reroft.fsm.{ApplyAsync, Initialize}
import com.github.rerorero.reroft.log.{LogEntry, LogRepository}
import com.github.rerorero.reroft.test.TestUtil
import com.github.rerorero.reroft.{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

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
    shutdown()
  }

  implicit def arbNode: Arbitrary[Node] = Arbitrary(Gen.const(Node(sample[NodeID], TestProbe().ref)))
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer =  ActorMaterializer()

  def nodesForTest(len: Int): Seq[(Node, TestProbe)] = (1 to len).map{_ =>
    val probe = TestProbe()
    (Node(sample[NodeID], probe.ref), probe)
  }

  class MockedRaftActor(
    val nodes: Set[Node] = sampleN[Node](3).toSet,
    val myID: NodeID = sample[NodeID],
    val minElectionTimeoutMS: Int = 150,
    val maxElectionTimeoutMS: Int = 300,
    val heartbeatIntervalMS: Int = 15,
  ) {
    val stateMachine = TestProbe()
    val logRepo = mock(classOf[LogRepository])

    val sut = TestFSMRef(new RaftActor(
      stateMachine.ref,
      logRepo,
      nodes,
      myID,
      minElectionTimeoutMS,
      maxElectionTimeoutMS,
      heartbeatIntervalMS,
    ))
  }

  "RaftServerImpl" should {
    "be initialized" in {
      val mockedSM = TestProbe()
      val sut = TestFSMRef(new RaftActor(mockedSM.ref, sample[LogRepository], sample[Set[Node]], sample[NodeID]))
      assert(sut.stateName === Follower)
    }
  }

  "follower" should {
    "start election when no append entries receives" in  {
      val timeout = 100
      val m = new MockedRaftActor(minElectionTimeoutMS = timeout, maxElectionTimeoutMS = timeout +1)
      m.sut.setState(Follower, RaftState.empty, 10 millisecond)
      assert(m.sut.stateName === Follower)
      Thread.sleep((timeout * 1.5).toLong)
      assert(m.sut.stateName === Candidate)
    }

    "reject AppendEntries when term is stale" in  {
      val m = new MockedRaftActor()
      val state = RaftState.empty.copy(currentTerm = 100L)
      m.sut.setState(Follower, state, 10 millisecond)

      m.sut ! sample[AppendEntriesRequest].withTerm(99L)
      expectMsg(AppendEntriesResponse(100L, false))
    }

    "remove all when receives zero prevIndex" in  {
      val m = new MockedRaftActor()
      val state = RaftState.empty.copy(currentTerm = 100L)
      m.sut.setState(Follower, state, 10 millisecond)

      m.sut ! sample[AppendEntriesRequest]
        .withPrevLogIndex(0L)
        .withTerm(101L)
        .withEntries(Seq.empty)
      expectMsg(AppendEntriesResponse(101L, true))
      m.stateMachine.expectMsg(Initialize)
      verify(m.logRepo, times(1)).empty()
      assert(m.sut.stateData.currentTerm === 101L)
    }

    "reject if it has no logs" in  {
      val m = new MockedRaftActor()
      val state = RaftState.empty.copy(currentTerm = 100L)
      m.sut.setState(Follower, state, 10 millisecond)
      when(m.logRepo.contains(any[Long], any[Long])).thenReturn(false)

      m.sut ! sample[AppendEntriesRequest]
        .withPrevLogIndex(123L)
        .withTerm(100L)
      expectMsg(AppendEntriesResponse(100L, false))
    }

    "append logs and commit" in  {
      val m = new MockedRaftActor()
      val state = RaftState.empty.copy(currentTerm = 100L)
      m.sut.setState(Follower, state, 10 millisecond)
      when(m.logRepo.contains(any[Long], any[Long])).thenReturn(true)
      when(m.logRepo.getCommitIndex()).thenReturn(9L)

      val req = sample[AppendEntriesRequest]
        .withPrevLogIndex(123L)
        .withTerm(100L)
        .withLeaderCommit(10L)
      m.sut ! req

      expectMsg(AppendEntriesResponse(100L, true))
      verify(m.logRepo, times(1)).removeConflicted(req.prevLogTerm, req.prevLogIndex)
      verify(m.logRepo, times(1)).append(req.entries.map(LogEntry.fromMessage))
      verify(m.logRepo, times(1)).commit(10L)
      m.stateMachine.expectMsg(ApplyAsync(10L))
    }

    "append logs without commit" in  {
      val m = new MockedRaftActor()
      val state = RaftState.empty.copy(currentTerm = 100L)
      m.sut.setState(Follower, state, 10 millisecond)
      when(m.logRepo.contains(any[Long], any[Long])).thenReturn(true)
      when(m.logRepo.getCommitIndex()).thenReturn(10L)

      val req = sample[AppendEntriesRequest]
        .withPrevLogIndex(123L)
        .withTerm(100L)
        .withLeaderCommit(10L)
      m.sut ! req

      expectMsg(AppendEntriesResponse(100L, true))
      verify(m.logRepo, times(1)).removeConflicted(req.prevLogTerm, req.prevLogIndex)
      verify(m.logRepo, times(1)).append(req.entries.map(LogEntry.fromMessage))
      verify(m.logRepo, times(0)).commit(any[Long])
      m.stateMachine.expectMsg(ApplyAsync(10L))
    }
  }

  "candidate" should {
    "start election" in {
      val nodes = nodesForTest(3)
      val m = new MockedRaftActor(nodes = nodes.map(_._1).toSet, myID = nodes.head._1.id, minElectionTimeoutMS = 100, maxElectionTimeoutMS = 101)
      m.sut.setState(Follower, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)
      when(m.logRepo.lastLogIndex()).thenReturn(234L)
      when(m.logRepo.lastLogTerm()).thenReturn(567L)

      Thread.sleep(120)
      assert(m.sut.stateName === Candidate)

      assert(m.sut.stateData.currentTerm === 11L)
      nodes.head._2.expectNoMessage(300 millisecond)
      nodes.filter(_._1.id != m.myID).foreach(n => n._2.expectMsg(RequestVoteRequest(11L, m.myID.toString(), 234L, 567L)))
    }

    "reject vote if term is stale" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)

      m.sut ! RequestVoteRequest(9L, "hoge:1111", 3L, 4L)
      expectMsg(RequestVoteResponse(10L, voteGranted = false))
      assert(m.sut.stateName === Candidate)
    }
  }
}
