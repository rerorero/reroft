package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, Materializer}
import com.github.rerorero.reroft._
import com.github.rerorero.reroft.fsm.StateMachine
import com.github.rerorero.reroft.log.logRepositoryDummy
import com.google.common.net.HostAndPort

import scala.concurrent.{ExecutionContext, Future}

case class ServerConfig(
  clusterNodes: Set[HostAndPort],
  me: HostAndPort
)

class Server(system: ActorSystem, config: ServerConfig) {
  def run(port: Int): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = sys.dispatcher

    val stateMachine = sys.actorOf(StateMachine.props())
    val raftFSM = sys.actorOf(RaftActor.props(
      stateMachine,
      logRepositoryDummy,// TODO: dummy should be replaced
      config.clusterNodes.map(Node.fromAddress),
      NodeID(config.me))
    )

    Http().bindAndHandleAsync(
      RaftServiceHandler(new RaftServiceImpl(raftFSM)),
      interface = "127.0.0.1",
      port = port
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
