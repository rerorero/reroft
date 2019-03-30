package com.github.rerorero.reroft.calc

import com.github.rerorero.reroft.Agent
import com.github.rerorero.reroft.raft.RaftConfig
import com.github.rerorero.reroft.test.TestUtil
import com.google.common.net.HostAndPort
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

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

      Thread.sleep(3000)
      val s = Await.result(client.runStatCommand(), 3000 millis)
      println(Agent.statPrettyPrint(s))
      servers.map(_.map(_.unbind()))
    }
  }
}
