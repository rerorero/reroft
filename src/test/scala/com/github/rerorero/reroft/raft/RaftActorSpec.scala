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
    val nodes: Set[Node] = (1 to 3).map(_ => sample[Node]).toSet,
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
      assert(m.sut.stateData.votedFor === None)
    }

    "reject vote if my log is advanced" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)
      when(m.logRepo.lastLogIndex()).thenReturn(100L)
      when(m.logRepo.lastLogTerm()).thenReturn(10L)

      m.sut ! RequestVoteRequest(10L, "hoge:1111", 99L, 11L)
      expectMsg(RequestVoteResponse(10L, voteGranted = false))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.votedFor === None)

      m.sut ! RequestVoteRequest(10L, "hoge:1111", 101L, 9L)
      expectMsg(RequestVoteResponse(10L, voteGranted = false))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.votedFor === None)
    }

    "accept vote" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)
      when(m.logRepo.lastLogIndex()).thenReturn(100L)
      when(m.logRepo.lastLogTerm()).thenReturn(10L)

      m.sut ! RequestVoteRequest(10L, "hoge:1111", 100L, 11L)
      expectMsg(RequestVoteResponse(10L, voteGranted = true))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.votedFor === Some(NodeID.of("hoge:1111")))

      // once voted, reject new vote even if it contains newer index
      m.sut ! RequestVoteRequest(10L, "hoge:2222", 200L, 20L)
      expectMsg(RequestVoteResponse(10L, voteGranted = false))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.votedFor === Some(NodeID.of("hoge:1111")))
    }

    "become follower when discovered stale term" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)

      m.sut ! VoteResponse(NodeID.of("localhost:2222"),RequestVoteResponse(11L, true))

      expectNoMessage(100 millisecond)
      assert(m.sut.stateName === Follower)
      assert(m.sut.stateData.votedFor === None)
      assert(m.sut.stateData.granted.isEmpty)
    }

    "ignore response which contains stale term" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)

      m.sut ! VoteResponse(NodeID.of("localhost:2222"),RequestVoteResponse(9L, true))

      expectNoMessage(100 millisecond)
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.granted.isEmpty)
    }

    "ignore response whose granted is false " in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)

      m.sut ! VoteResponse(NodeID.of("localhost:2222"),RequestVoteResponse(10L, false))

      expectNoMessage(100 millisecond)
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.granted.isEmpty)
    }

    "complete vote if accepted by majority" in {
      val nodes = (1 to 5).map(_ => sample[Node])
      val myID = nodes.reverse.head.id
      val m = new MockedRaftActor(nodes = nodes.toSet, myID = myID, minElectionTimeoutMS = 1000, maxElectionTimeoutMS = 1001)
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)

      // 1
      m.sut ! VoteResponse(nodes(0).id, RequestVoteResponse(10L, true))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.leaderID === None)
      assert(m.sut.stateData.granted === Set(nodes(0).id))

      // 2
      m.sut ! VoteResponse(nodes(1).id, RequestVoteResponse(10L, true))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.leaderID === None)
      assert(m.sut.stateData.granted === Set(nodes(0).id, nodes(1).id))

      // 3, majority
      m.sut ! VoteResponse(nodes(2).id, RequestVoteResponse(10L, true))
      assert(m.sut.stateName === Leader)
      assert(m.sut.stateData.leaderID === Some(myID))
      assert(m.sut.stateData.granted === Set(nodes(0).id, nodes(1).id, nodes(2).id))
    }

    "restart election if timouted" in {
      val nodes = nodesForTest(3)
      val timeout = 100
      val m = new MockedRaftActor(nodes = nodes.map(_._1).toSet, myID = nodes.head._1.id, minElectionTimeoutMS = timeout, maxElectionTimeoutMS = timeout+1)
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)
      when(m.logRepo.lastLogIndex()).thenReturn(234L)
      when(m.logRepo.lastLogTerm()).thenReturn(567L)

      // 1
      m.sut ! VoteResponse(NodeID.of("localhost:1234"), RequestVoteResponse(10L, true))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.leaderID === None)
      assert(m.sut.stateData.granted === Set(NodeID.of("localhost:1234")))

      // timeout
      Thread.sleep((timeout * 1.2).toInt)
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.granted === Set())
      // re-voted with incremented term
      nodes.filter(_._1.id != m.myID).foreach(n => n._2.expectMsg(RequestVoteRequest(11L, m.myID.toString(), 234L, 567L)))
    }

    "become follower when discovered stale term by AppendEntriesRequest" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)
      when(m.logRepo.contains(any[Long], any[Long])).thenReturn(true)
      when(m.logRepo.getCommitIndex()).thenReturn(9L)

      m.sut ! AppendEntriesRequest(11L, "localhost:2222", 123L, 10L, Seq(), 1L)

      expectMsg(AppendEntriesResponse(11L, true))
      assert(m.sut.stateName === Follower)
      assert(m.sut.stateData.leaderID === Some(NodeID.of("localhost:2222")))
    }

    "ignores AppendEntriesRequest whose term is old" in {
      val m = new MockedRaftActor()
      m.sut.setState(Candidate, RaftState.empty.copy(currentTerm = 10L), 10 millisecond)

      m.sut ! AppendEntriesRequest(9L, "localhost:2222", 1L, 2L, Seq(), 1L)

      expectMsg(AppendEntriesResponse(10L, false))
      assert(m.sut.stateName === Candidate)
      assert(m.sut.stateData.leaderID === None)
    }
  }

  "leader" should {
    "start appending log process" in {
      val nodes = nodesForTest(3)
      val myID = nodes.head._1.id
      val m = new MockedRaftActor(nodes = nodes.map(_._1).toSet, myID, heartbeatIntervalMS = 1000)
      val state = RaftState.empty.copy(
        currentTerm = 10L,
        leaderID = Some(myID),
        nextIndex = Map(
          nodes(1)._1.id -> 1L,
          nodes(2)._1.id -> 2L,
        )
      )
      when(m.logRepo.getLogs(any[Long])).thenReturn(Seq(LogEntry(8L, 3L)))
      when(m.logRepo.getCommitIndex()).thenReturn(2L)

      m.sut.setState(Leader, state, 10 millisecond)
      m.sut ! BroadcastAppendLog

      nodes(1)._2.expectMsg(AppendEntriesRequest(10L, state.leaderID.get.toString(), 1L, 8L, Seq(LogEntry(8L, 3L).toMessage), 2L))
      nodes(2)._2.expectMsg(AppendEntriesRequest(10L, state.leaderID.get.toString(), 2L, 8L, Seq(LogEntry(8L, 3L).toMessage), 2L))
      nodes(0)._2.expectNoMessage(100 millisecond)
      assert(m.sut.stateData.matchIndex === Some(Map.empty))

      // ignore until responses are collected
      m.sut ! BroadcastAppendLog
      nodes.foreach(_._2.expectNoMessage(50 millis))
      assert(m.sut.stateData.matchIndex === Some(Map.empty))
    }

    "continue to collect until AppendEntriesResponses are received by majority" in {
      val nodes = nodesForTest(5)
      val myID = nodes.head._1.id
      val m = new MockedRaftActor(nodes = nodes.map(_._1).toSet, myID, heartbeatIntervalMS = 1000)
      val nextIndex = Map(
          nodes(1)._1.id -> 1L,
          nodes(2)._1.id -> 2L,
          nodes(3)._1.id -> 3L,
          nodes(4)._1.id -> 4L,
        )
      val state = RaftState.empty.copy(
        currentTerm = 10L,
        leaderID = Some(myID),
        matchIndex = Some(Map.empty),
        nextIndex = nextIndex,
      )
      when(m.logRepo.getLogs(any[Long])).thenReturn(Seq(LogEntry(8L, 8L), LogEntry(8L, 9L)))
      when(m.logRepo.getCommitIndex()).thenReturn(2L)
      m.sut.setState(Leader, state, 10 millisecond)

      // receive from 1
      m.sut ! AppendResponse(nodes(1)._1.id, AppendEntriesResponse(10L, true), 1L, Some(9L))
      assert(m.sut.stateData.matchIndex === Some(Map(nodes(1)._1.id -> 9L)))
      assert(m.sut.stateData.nextIndex === Map(
        nodes(1)._1.id -> 9L,
        nodes(2)._1.id -> 2L,
        nodes(3)._1.id -> 3L,
        nodes(4)._1.id -> 4L,
      ))
      verify(m.logRepo, never()).commit(any[Long])

      // receive from 2
      m.sut ! AppendResponse(nodes(2)._1.id, AppendEntriesResponse(10L, true), 2L, Some(10L))
      assert(m.sut.stateData.matchIndex === Some(Map(
        nodes(1)._1.id -> 9L,
        nodes(2)._1.id -> 10L,
      )))
      assert(m.sut.stateData.nextIndex === Map(
        nodes(1)._1.id -> 9L,
        nodes(2)._1.id -> 10L,
        nodes(3)._1.id -> 3L,
        nodes(4)._1.id -> 4L,
      ))
      verify(m.logRepo, never()).commit(any[Long])

      // receive from 2
      m.sut ! AppendResponse(nodes(3)._1.id, AppendEntriesResponse(10L, true), 3L, Some(11L))
      assert(m.sut.stateData.matchIndex === None)
      assert(m.sut.stateData.nextIndex === Map(
        nodes(1)._1.id -> 9L,
        nodes(2)._1.id -> 10L,
        nodes(3)._1.id -> 11L,
        nodes(4)._1.id -> 4L,
      ))
      verify(m.logRepo, times(1)).commit(9L)
    }

    "become follower when it discovers stale" in {
      val m = new MockedRaftActor(heartbeatIntervalMS = 1000)
      when(m.logRepo.getLogs(any[Long])).thenReturn(Seq(LogEntry(8L, 8L), LogEntry(8L, 9L)))
      when(m.logRepo.getCommitIndex()).thenReturn(2L)
      val state = RaftState.empty.copy(
        currentTerm = 10L,
        matchIndex = Some(Map.empty),
        nextIndex = Map(m.nodes.head.id -> 0L)
      )
      m.sut.setState(Leader, state, 10 millisecond)

      m.sut ! AppendResponse(m.nodes.head.id, AppendEntriesResponse(12L, true), 1L, Some(9L))
      assert(m.sut.stateName === Follower)
    }

    "retry with decremented index when request is rejected" in {
      val nodes = nodesForTest(5)
      val myID = nodes.head._1.id
      val m = new MockedRaftActor(nodes = nodes.map(_._1).toSet, myID = myID, heartbeatIntervalMS = 1000)
      when(m.logRepo.getLogs(any[Long])).thenReturn(Seq(LogEntry(8L, 8L), LogEntry(8L, 9L)))
      when(m.logRepo.getCommitIndex()).thenReturn(2L)
      val state = RaftState.empty.copy(
        currentTerm = 10L,
        matchIndex = Some(Map.empty),
        nextIndex = Map(nodes(1)._1.id -> 5L)
      )
      m.sut.setState(Leader, state, 10 millisecond)

      m.sut ! AppendResponse(nodes(1)._1.id, AppendEntriesResponse(10L, false), 5L, Some(1L))
      nodes(1)._2.expectMsg(AppendEntriesRequest(10L, myID.toString(), 4L, 8L, Seq(LogEntry(8L, 8L), LogEntry(8L, 9L)).map(_.toMessage), 2L))
    }

    "become follower when it receives heartbeat contains newer term" in {
      val m = new MockedRaftActor(heartbeatIntervalMS = 1000)
      val state = RaftState.empty.copy(
        currentTerm = 10L,
        nextIndex = m.nodes.map(n => (n.id, 0L)).toMap,
      )
      when(m.logRepo.getLogs(any[Long])).thenReturn(Seq())
      when(m.logRepo.getCommitIndex()).thenReturn(2L)
      m.sut.setState(Leader, state, 10 millisecond)

      m.sut ! AppendEntriesRequest(11L, "localhost:2222", 0L, 0L, Seq.empty, 0L)

      assert(m.sut.stateName === Follower)
      assert(m.sut.stateData.currentTerm === 11L)
      expectMsg(AppendEntriesResponse(11L, true))
    }

    "reject AppendEntriesRequest which has an old term" in {
      val m = new MockedRaftActor(heartbeatIntervalMS = 1000)
      val state = RaftState.empty.copy(
        currentTerm = 10L,
        nextIndex = m.nodes.map(n => (n.id, 0L)).toMap,
      )
      when(m.logRepo.getLogs(any[Long])).thenReturn(Seq())
      when(m.logRepo.getCommitIndex()).thenReturn(2L)
      m.sut.setState(Leader, state, 10 millisecond)

      m.sut ! AppendEntriesRequest(9L, "localhost:2222", 0L, 0L, Seq.empty, 0L)

      assert(m.sut.stateName === Leader)
      assert(m.sut.stateData.currentTerm === 10L)
      expectMsg(AppendEntriesResponse(10L, false))
    }
  }
}
