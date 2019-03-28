package com.github.rerorero.reroft.calc

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.github.rerorero.reroft.fsm.{Apply, ApplyResult, Initialize}
import com.github.rerorero.reroft.grpc.calc.{CalcEntry, CalcResult, Command}
import com.github.rerorero.reroft.logs.LogRepoEntry
import com.github.rerorero.reroft.test.TestUtil
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class StateMachineSpec
  extends TestKit(ActorSystem("fsm"))
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

  "StateMachine" should {
    "apply" in {
      val logs = mock(classOf[Logs])
      val sut = TestActorRef(new StateMachine(logs))
      when(logs.getLogs(1L, Some(100L))).thenReturn(Seq(
        LogRepoEntry(1L, 10L, CalcEntry(Command.ADD, 10)),  // 10
        LogRepoEntry(1L, 11L, CalcEntry(Command.SUB, 4)),   // 6
        LogRepoEntry(1L, 12L, CalcEntry(Command.MULTI, 10)),   // 60
        LogRepoEntry(1L, 13L, CalcEntry(Command.DIV, 4)),   // 15
      ))
      when(logs.getLogs(101L, Some(101L))).thenReturn(Seq(
        LogRepoEntry(1L, 14L, CalcEntry(Command.ADD, 10)),  // 25
      ))

      sut ! Apply(100)
      expectMsg(ApplyResult(CalcResult(15.0), 100L))
      verify(logs, times(1)).getLogs(any[Long], any[Option[Long]])

      sut ! Apply(100)
      expectMsg(ApplyResult(CalcResult(15.0), 100L))
      verify(logs, times(1)).getLogs(any[Long], any[Option[Long]])

      sut ! Apply(101)
      expectMsg(ApplyResult(CalcResult(25.0), 101L))
      verify(logs, times(2)).getLogs(any[Long], any[Option[Long]])

      sut ! Initialize
      sut ! Apply(100)
      expectMsg(ApplyResult(CalcResult(15.0), 100L))
      verify(logs, times(3)).getLogs(any[Long], any[Option[Long]])
    }
  }
}
