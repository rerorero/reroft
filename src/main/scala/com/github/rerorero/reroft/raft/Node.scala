package com.github.rerorero.reroft.raft

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.github.rerorero.reroft.{RaftService, RaftServiceClient}
import com.google.common.net.HostAndPort

import scala.concurrent.ExecutionContext

case class NodeID(addr: HostAndPort)

case class Node(
  id: NodeID,
  client: RaftService
)

object Node {
  def fromAddress(address: HostAndPort)(implicit mat: Materializer, ec: ExecutionContext, actor: ActorSystem): Node = {
      val client = RaftServiceClient(
        GrpcClientSettings
          .connectToServiceAt(address.getHost, address.getPort)
          .withTls(false)
      )
      Node(NodeID(address), client)
  }
}