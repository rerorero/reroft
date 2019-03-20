package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.pattern.ask
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import com.github.rerorero.reroft._
import com.github.rerorero.reroft.fsm.{StateMachine, applierDummy}
import com.github.rerorero.reroft.log.logRepositoryDummy
import com.google.common.net.HostAndPort

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class ServerConfig(
  clusterNodes: Set[HostAndPort],
  me: HostAndPort
) {
  def toConfigure(implicit mat: Materializer, system: ActorSystem, ec: ExecutionContext): Configure =
    Configure(
      clusterNodes.map(Node.fromAddress),
      NodeID(me),
    )
}

class Server(system: ActorSystem, config: ServerConfig) {
  def run(port: Int): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = sys.dispatcher
    implicit val timeout = Timeout(5 seconds)

    val stateMachine = sys.actorOf(StateMachine.props(applierDummy))
    val raftFSM = sys.actorOf(RaftActor.props(stateMachine, logRepositoryDummy)) // TODO: dummy should be replaced

    (raftFSM ? config.toConfigure).flatMap( _ =>
      Http().bindAndHandleAsync(
        RaftServiceHandler(new RaftServiceImpl(raftFSM)),
        interface = "127.0.0.1",
        port = port
      ).map { b =>
        println(s"raft server bound to ${b.localAddress}")
        b
      }
    )
  }
}

class RaftServiceImpl(raftFSM: ActorRef)(implicit mat: Materializer) extends RaftService {
  import mat.executionContext

  override def appendEntries(in: AppendEntriesRequest): Future[AppendEntriesResponse] = {
    println("natoring: append: " + in.toString())
    return Future(AppendEntriesResponse())
  }

  override def requestVote(in: RequestVoteRequest): Future[RequestVoteResponse] = {
    println("natoring: request: " + in.toString())
    return Future(RequestVoteResponse())
  }
}
