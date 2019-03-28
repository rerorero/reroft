package com.github.rerorero.reroft.calc

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.github.rerorero.reroft.grpc.calc.CalcEntry
import com.github.rerorero.reroft.logs.{LogRepoEntry, LogRepository}
import com.github.rerorero.reroft.test.TestUtil
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

class LogsSpec
    extends TestKit(ActorSystem("logs"))
    with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with CalcArbitraries
    with TestUtil {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    shutdown()
  }

  "Logs" should {
    "initialize" in {
      val sut = new Logs()
      assert(sut.getLogs(0L) === Seq(Logs.origin))
      assert(sut.getLogs(1L) === Seq.empty[LogRepository[CalcEntry]])
      assert(sut.lastLogIndex() === 0L)
      assert(sut.lastLogTerm() === 0L)
    }

    "append and removeConflicted" in {
      val sut = new Logs()
      val logs = Seq(
        LogRepoEntry(index = 100L, term = 10L, entry = sample[CalcEntry]),
        LogRepoEntry(index = 101L, term = 10L, entry = sample[CalcEntry]),
        LogRepoEntry(index = 103L, term = 11L, entry = sample[CalcEntry]),
        LogRepoEntry(index = 200L, term = 11L, entry = sample[CalcEntry]),
        LogRepoEntry(index = 201L, term = 12L, entry = sample[CalcEntry]),
      )

      sut.append(logs)
      assert(sut.getLogs(103) === Seq(logs(2),logs(3), logs(4)))
      assert(sut.getLogs(101, Some(200)) === Seq(logs(1),logs(2), logs(3)))
      assert(sut.lastLogTerm() === 12L)
      assert(sut.lastLogIndex() === 201L)

      logs.foreach(l => assert(sut.contains(l.term, l.index)))
      assert(!sut.contains(100L, 9L))
      assert(!sut.contains(99L, 10L))

      // no conflicted
      sut.removeConflicted(12L, 201L)
      assert(sut.getLogs(100L) === logs)

      // 200 and 201 are conflicted
      sut.removeConflicted(11L, 103L)
      assert(sut.getLogs(100L) === Seq(logs(0), logs(1), logs(2)))
      assert(!sut.contains(200L, 11L))
      assert(sut.lastLogIndex() === 103L)
      assert(sut.lastLogTerm() === 11L)

      // commit
      assert(sut.getCommitIndex() === 0L)
      sut.commit(101L)
      assert(sut.getCommitIndex() === 101L)

      // empty
      sut.empty()
      assert(sut.getLogs(0L) === Seq(Logs.origin))
      assert(sut.lastLogIndex() === 0L)
      assert(sut.lastLogTerm() === 0L)
      assert(sut.getCommitIndex() === 0L)
    }
  }
}

