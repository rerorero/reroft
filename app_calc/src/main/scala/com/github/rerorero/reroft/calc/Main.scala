package com.github.rerorero.reroft.calc

import akka.actor.ActorRef
import com.github.rerorero.reroft.Agent
import com.github.rerorero.reroft.grpc.calc.{CalcEntry, CalcResult}
import com.github.rerorero.reroft.logs.LogRepository
import com.github.rerorero.reroft.raft.RaftConfig
import com.google.common.net.HostAndPort
import scalapb.GeneratedMessageCompanion

object main {
  def main(args: Array[String]): Unit = {
    for {
      nodes <- sys.props.get("nodes").map(_.split(",").map(HostAndPort.fromString).toSet)
      me <- sys.props.get("me").map(HostAndPort.fromString)
      mode <- sys.props.get("mode") flatMap  {
        case "server" => ???
        case "client" => ???
        case _ => None
      }
    } yield {
      val conf = RaftConfig(nodes, me)
    }
  }

  def exitOnErr(msg: String): Unit = {
    println(msg)
    sys.exit(1)
  }
}

class CalcAgent extends Agent[CalcEntry, CalcResult] {

  override def logRepository: LogRepository[CalcEntry] = ???

  override def config: RaftConfig = ???

  override def stateMachine: ActorRef = ???

  override implicit def cmpEntry: GeneratedMessageCompanion[CalcEntry] = ???

  override implicit def cmpComputed: GeneratedMessageCompanion[CalcResult] = ???
}
