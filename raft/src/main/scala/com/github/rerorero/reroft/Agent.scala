package com.github.rerorero.reroft

import akka.actor.{ActorRef, ActorSystem}
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, Materializer}
import com.github.rerorero.reroft.grpc.{ClientCommandRequest, ClientCommandResponse, RaftServiceClient}
import com.github.rerorero.reroft.logs.LogRepository
import com.github.rerorero.reroft.raft.RaftConfig
import com.google.common.net.HostAndPort
import com.google.protobuf.any.Any
import com.typesafe.config.ConfigFactory
import io.grpc.{Status, StatusRuntimeException}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

abstract class Agent[Entry <: GeneratedMessage with Message[Entry], Computed <: GeneratedMessage with Message[Computed]] {
  def logRepository: LogRepository[Entry]

  def config: RaftConfig

  def stateMachine: ActorRef

  implicit def cmpEntry: GeneratedMessageCompanion[Entry]

  implicit def cmpComputed: GeneratedMessageCompanion[Computed]

  implicit val system = ActorSystem(
    "reroft",
    ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
      .withFallback(ConfigFactory.defaultApplication())
  )
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  private[this] val clientMap = collection.mutable.Map.empty[HostAndPort, RaftServiceClient]

  def getCLient(hostAndPort: HostAndPort): RaftServiceClient = synchronized {
    clientMap.getOrElseUpdate(hostAndPort, RaftServiceClient(
      GrpcClientSettings.connectToServiceAt(hostAndPort.getHost, hostAndPort.getPort)
        .withTls(false)
    ))
  }

  def runServer(): Future[Http.ServerBinding] =
    new Server[Entry, Computed](system, stateMachine, logRepository, config).run(config.me.getPort)

  def runClientCommand(command: Seq[Entry]): Future[Computed] = {
    val client = Random.shuffle(config.clusterNodes).head
    val req = ClientCommandRequest(command.map(Any.pack[Entry]))
    getCLient(client).clientCommand(req).flatMap {
      case ClientCommandResponse(Some(computed), _) =>
        Future.successful(computed.unpack[Computed])
      case ClientCommandResponse(None, redirectTo) =>
        getCLient(HostAndPort.fromString(redirectTo)).clientCommand(req).map(_.computed.get.unpack[Computed])
    }
  }
}
