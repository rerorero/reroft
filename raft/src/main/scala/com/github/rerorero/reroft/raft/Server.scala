package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, Materializer}
import com.github.rerorero.reroft._
import com.google.common.net.HostAndPort
import akka.pattern.ask
import akka.util.Timeout
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

case class ServerConfig(
  clusterNodes: Set[HostAndPort],
  me: HostAndPort
)

class Server(system: ActorSystem, config: ServerConfig) {
  def run(port: Int): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = sys.dispatcher

//    val stateMachine = null // TODO
//    val raftFSM = sys.actorOf(RaftActor.props(
//      stateMachine,
//      logRepositoryDummy,// TODO: dummy should be replaced
//      config.clusterNodes.map(Node.fromAddress),
//      NodeID(config.me))
//    )


    Http().bindAndHandleAsync(
      RaftServiceHandler(new RaftServiceImpl(null)),
      interface = "127.0.0.1",
      port = port
    )
  }
}

class RaftServiceImpl(raftFSM: ActorRef)(implicit ec: ExecutionContext) extends RaftService {
  implicit val askTimeout: Timeout = 10 seconds

  override def appendEntries(in: AppendEntriesRequest): Future[AppendEntriesResponse] =
    (raftFSM ? in).mapTo[AppendEntriesResponse]

  override def requestVote(in: RequestVoteRequest): Future[RequestVoteResponse] =
    (raftFSM ? in).mapTo[RequestVoteResponse]

  override def clientCommand(in: ClientCommandRequest): Future[ClientCommandResponse] =
    (raftFSM ? in).mapTo[ClientResponse].flatMap{
      case ClientSuccess(res) => Future.successful(res)
      case ClientRedirect(leader: NodeID) => Future.failed(new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(leader.toString())))
      case ClientFailure(e: Throwable) => Future.failed(e)
    }
}
