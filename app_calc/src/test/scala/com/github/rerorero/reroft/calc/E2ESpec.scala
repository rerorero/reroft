package com.github.rerorero.reroft.calc

import java.util.concurrent.atomic.AtomicBoolean

import akka.pattern.ask
import akka.util.Timeout
import com.github.rerorero.reroft.Agent
import com.github.rerorero.reroft.grpc.StatCommandResponse
import com.github.rerorero.reroft.grpc.calc.{CalcEntry, Command}
import com.github.rerorero.reroft.raft.{Candidate, Leader, RaftConfig}
import com.github.rerorero.reroft.test.TestUtil
import com.google.common.net.HostAndPort
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class E2ESpec
  extends Matchers
  with WordSpecLike
  with BeforeAndAfterAll
  with ScalaFutures
  with CalcArbitraries
  with TestUtil {

  implicit val ex: ExecutionContext = ExecutionContext.global

  "calc command" should {
    "run" in  {
      val nodes = unusedPorts(5).map(port => HostAndPort.fromString(s"127.0.0.1:${port}"))
      val agents = nodes.map(node => new CalcAgent(RaftConfig(nodes.toSet, node)))
      val servers = agents.map(_.runServer())
      val client = new CalcAgent(RaftConfig(nodes.toSet, HostAndPort.fromString("localhost")))

      // waiting for election completed
      def getState(): StatCommandResponse = {
        val timeout = new AtomicBoolean(false)
        def stat() = client.runStatCommand().flatMap {
          case r if r.nodes.exists(_.state == Leader.toString) && !r.nodes.exists(_.state == Candidate.toString) => Future(r)
          case _ => Future.failed(new Exception("election has not completed"))
        }
        def pollStat(): Future[StatCommandResponse]  = stat().recoverWith {
          case e if timeout.get() => Future.failed(e)
          case e => Future.failed(e)
            Thread.sleep(100)
            pollStat()
        }
        val state = Await.result(pollStat(), 10000 millis)
        timeout.set(true)
        state
      }

      // append logs
      val entries = Seq(
        CalcEntry(Command.ADD, 10.0),
        CalcEntry(Command.SUB, 5.0),
        CalcEntry(Command.MULTI, 8.0),
        CalcEntry(Command.DIV, 4.0),
      )
      val computed = Await.result(client.runClientCommand(entries), 10000 millis)
      println(computed)
      Thread.sleep(1000)

      val leaderState = getState().nodes.find(_.state == Leader.toString).get
      println(Agent.statPrettyPrint(getState()))
      implicit val askTimeout: Timeout = 1000.millis
      agents.foreach { a =>
        val GetComputedRes(applyIndex, nodeComputed) = Await.result((a.stateMachine ? GetComputed).mapTo[GetComputedRes], 1000 millis)
        assert(a.logrepo.getCommitIndex() === leaderState.commitIndex)
        assert(a.logrepo.lastLogIndex() === leaderState.lastLogIndex)
        assert(a.logrepo.lastLogTerm() === leaderState.lastLogTerm)
        assert(applyIndex === leaderState.lastLogIndex)
        assert(computed.result === nodeComputed)
      }

      servers.foreach(_.map(_.unbind()))
    }
  }
}
