package com.github.rerorero.reroft.grpc

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, Materializer}
import com.github.rerorero.reroft._
import com.github.rerorero.reroft.fsm.StateMachine
import com.github.rerorero.reroft.log.logRepositoryDummy
import com.github.rerorero.reroft.raft.{Node, NodeID, RaftActor}
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

class RaftServiceImpl(raftFSM: ActorRef) extends RaftService {
  override def appendEntries(in: AppendEntriesRequest): Future[AppendEntriesResponse] = ???
  override def requestVote(in: RequestVoteRequest): Future[RequestVoteResponse] = ???
  override def clientCommand(in: ClientCommandRequest): Future[ClientCommandResponse] = ???
}
