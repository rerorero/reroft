package com.github.rerorero.reroft

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.pattern.ask
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import com.github.rerorero.reroft.grpc._
import com.github.rerorero.reroft.logs.LogRepository
import com.github.rerorero.reroft.raft._
import io.grpc.{Status, StatusRuntimeException}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class Server[Entry <: GeneratedMessage with Message[Entry], Computed <: GeneratedMessage with Message[Computed]](
  system: ActorSystem, stateMachine: ActorRef, logRepository: LogRepository[Entry], config: RaftConfig
)(implicit cmpEntry: GeneratedMessageCompanion[Entry]) {
  def run(port: Int): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = sys.dispatcher

    val raftFSM = sys.actorOf(RaftActor.props(
      stateMachine,
      logRepository,
      config.clusterNodes.map(Node.fromAddress),
      NodeID(config.me))
    )

    Http().bindAndHandleAsync(
      RaftServiceHandler(new RaftServiceImpl(raftFSM)),
      interface = "127.0.0.1", // TODO: to be configurable
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
