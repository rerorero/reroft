package com.github.rerorero.reroft.raft

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.github.rerorero.reroft._
import com.google.common.net.HostAndPort

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

case class NodeID(addr: HostAndPort) {
  override def toString(): String = s"${addr.getHost}:${addr.getPort}"
}

object NodeID {
  def of(s: String): NodeID = NodeID(HostAndPort.fromString(s))
}

case class Node(
  id: NodeID,
  actor: ActorRef,
)

case class VoteResponse(nodeId: NodeID, res: RequestVoteResponse)
case class AppendResponse(nodeId: NodeID, res: AppendEntriesResponse, requestedPrevIndex: Long, lastIndex: Option[Long])

object Node {
  def fromAddress(address: HostAndPort)(implicit mat: Materializer, ec: ExecutionContext, system: ActorSystem): Node = {
    val client = RaftServiceClient(
      GrpcClientSettings
        .connectToServiceAt(address.getHost, address.getPort)
        .withTls(false)
    )
    val id = NodeID(address)
    Node(id, system.actorOf(Props(new NodeActor(id, client))))
  }
}

class NodeActor(val nodeID: NodeID, val client: RaftService) extends Actor with ActorLogging {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  override def receive: Receive = {
    case req: RequestVoteRequest =>
      val parent = sender()
      client.requestVote(req).onComplete {
        case Success(res) => parent ! VoteResponse(nodeID, res)
        case Failure(e) => log.error(e, s"failed to vote to ${nodeID.addr}")
      }
      log.info(s"send vote request to ${nodeID}")

    case req: AppendEntriesRequest =>
      val parent = sender()
      val lastIndex = req.entries.lastOption.map(_.index)

      client.appendEntries(req).onComplete {
        case Success(res) => parent ! AppendResponse(nodeID, res, req.prevLogIndex, lastIndex)
        case Failure(e) => log.error(e, s"failed to appendEntries to ${nodeID.addr}")
      }
      log.info(s"send append entries request to ${nodeID}")
  }
}
