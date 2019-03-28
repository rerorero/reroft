package com.github.rerorero.reroft.calc

import com.github.rerorero.reroft.raft.RaftConfig
import com.github.rerorero.reroft.test.TestUtil
import com.google.common.net.HostAndPort
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Ignore, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext

@Ignore
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
      val nodes = unusedPorts(5).map(port => HostAndPort.fromString(s"127.0.0.2:${port}")).toSet
      val agents = nodes.map(node => new CalcAgent(RaftConfig(nodes, node)))
      val servers = agents.map(_.runServer())

      Thread.sleep(10000)
      servers.map(_.map(_.unbind()))
    }
  }
}
