package com.github.rerorero.reroft.test

import com.github.rerorero.reroft._
import com.github.rerorero.reroft.log.LogRepository
import com.github.rerorero.reroft.raft._
import com.google.common.net.HostAndPort
import org.scalacheck.{Arbitrary, Gen}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import scala.concurrent.{ExecutionContext, Future}

trait ArbitrarySet {
  def sample[T](implicit arb: Arbitrary[T]): T = arb.arbitrary.sample.get

  // raft
  implicit val arbAppendEntriesRes: Arbitrary[AppendEntriesResponse] = Arbitrary(Gen.resultOf(AppendEntriesResponse.apply _))
  implicit val arbVoteRes: Arbitrary[RequestVoteResponse] = Arbitrary(Gen.resultOf(RequestVoteResponse.apply _))
  implicit val arbRaftServiceClient: Arbitrary[RaftService] = Arbitrary{
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
    val m = mock(classOf[RaftService])
    when(m.appendEntries(any[AppendEntriesRequest])).thenReturn(Future(sample[AppendEntriesResponse]))
    when(m.requestVote(any[RequestVoteRequest])).thenReturn(Future(sample[RequestVoteResponse]))
    Gen.const(m)
  }

  implicit val arbLogRespotiry: Arbitrary[LogRepository] = Arbitrary {
    val m = mock(classOf[LogRepository])
    Gen.const(m)
  }

  implicit val arbHostAndPort: Arbitrary[HostAndPort] = Arbitrary {
    for {
      host <- Gen.alphaStr
      port <- Gen.choose(4000, 50000)
    } yield HostAndPort.fromString(s"${host}:${port}")
  }

  implicit val arbNodeId: Arbitrary[NodeID] = Arbitrary(Gen.resultOf(NodeID.apply _))
  implicit val arbNode: Arbitrary[Node] = Arbitrary(Gen.resultOf(Node.apply _))
  implicit val arbRaftState: Arbitrary[RaftState] = Arbitrary(Gen.resultOf(RaftState.apply _))

  implicit val arbConfigure: Arbitrary[Configure] = Arbitrary(Gen.resultOf(Configure.apply _))
}
