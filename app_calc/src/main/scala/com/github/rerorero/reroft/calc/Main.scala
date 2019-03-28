package com.github.rerorero.reroft.calc

import akka.actor.ActorRef
import com.github.rerorero.reroft.Agent
import com.github.rerorero.reroft.grpc.calc.{CalcEntry, CalcResult, Command}
import com.github.rerorero.reroft.logs.LogRepository
import com.github.rerorero.reroft.raft.RaftConfig
import com.google.common.net.HostAndPort
import scalapb.GeneratedMessageCompanion

import scala.util.{Failure, Success, Try}

object main {
  def main(args: Array[String]): Unit = {
    val opt = for {
      nodes <- sys.props.get("nodes").map(_.split(",").map(HostAndPort.fromString).toSet)
      me <- sys.props.get("me").map(HostAndPort.fromString)
      mode <- sys.props.get("mode")
    } yield (new CalcAgent(RaftConfig(nodes, me)), mode)

    (opt, clientOption) match {
      case (Some((agent, "server")), _) =>
        agent.runServer()
      case (Some((agent, "client")), Success(entry)) =>
        agent.runClientCommand(Seq(entry))
      case _ =>
        exitOnErr("unexpected arguments")
    }
  }

  def clientOption(): Try[CalcEntry] =
    (sys.props.get("command"), sys.props.get("value")) match {
      case (Some(s), Some(value)) =>
        for {
          cmd <- Try(Command.fromName(s).get)
          v <- Try(value.toDouble)
        } yield CalcEntry(cmd, v)
      case _ =>
        Failure(new Exception("unexpecgted command or value"))
    }

  def exitOnErr(msg: String): Unit = {
    println(msg)
    sys.exit(1)
  }
}

class CalcAgent(override val config: RaftConfig) extends Agent[CalcEntry, CalcResult] {
  val logrepo  = new Logs
  override val logRepository: LogRepository[CalcEntry] = logrepo
  override val stateMachine: ActorRef = system.actorOf(StateMachine.props(logrepo))
  override implicit val cmpEntry: GeneratedMessageCompanion[CalcEntry] = CalcEntry
  override implicit val cmpComputed: GeneratedMessageCompanion[CalcResult] = CalcResult
}
