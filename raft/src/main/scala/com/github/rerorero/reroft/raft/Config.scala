package com.github.rerorero.reroft.raft

import com.google.common.net.HostAndPort

case class RaftConfig(
  clusterNodes: Set[HostAndPort],
  me: HostAndPort
)

