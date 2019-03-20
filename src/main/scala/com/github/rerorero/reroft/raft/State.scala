package com.github.rerorero.reroft.raft

import com.google.common.net.HostAndPort

case class RaftState(
  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
  currentTerm: Long,
  clusterNodes: Set[Node],
  myID: NodeID,
  leaderID: Option[String],
)

object RaftState {
  def empty: RaftState = RaftState(
    currentTerm = 0L,
    clusterNodes = Set.empty[Node],
    myID = NodeID(HostAndPort.fromString("localhost:0")),
    leaderID = None,
  )
}
