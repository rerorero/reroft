package com.github.rerorero.reroft

import akka.actor.{ActorRef, ActorSystem}
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, Materializer}
import com.github.rerorero.reroft.grpc.{RaftService, RaftServiceClient}
import com.github.rerorero.reroft.raft.{LogRepository, NodeID, RaftConfig, Server}
import com.google.common.net.HostAndPort
import com.typesafe.config.ConfigFactory
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.concurrent.{ExecutionContext, Future}

abstract class Agent[Entry <: GeneratedMessage with Message[Entry], Computed <: GeneratedMessage with Message[Computed]] {
  def logRepository: LogRepository[Entry]
  def config: RaftConfig
  def stateMachine: ActorRef
  implicit def  cmpEntry: GeneratedMessageCompanion[Entry]

  implicit val system = ActorSystem("reroft", ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
    .withFallback(ConfigFactory.defaultApplication())
  )
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  private[this] val clientMap = collection.mutable.Map.empty[HostAndPort, RaftServiceClient]
  def getCLient(hostAndPort: HostAndPort): RaftServiceClient = synchronized {
    if (!clientMap.contains(hostAndPort)) {
      val c = RaftServiceClient(
        GrpcClientSettings.connectToServiceAt(hostAndPort.getHost, hostAndPort.getPort)
          .withTls(false)
      )
      clientMap(hostAndPort) = c
    }
    clientMap(hostAndPort)
  }

  def runServer(): Future[Http.ServerBinding] =
    new Server[Entry, Computed](system, stateMachine, logRepository, config).run(config.me.getPort)

  def runClientCommand(command: Seq[Entry]): Future[Computed] = {
    ???
  }
}
