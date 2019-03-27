package com.github.rerorero.reroft.test

import java.net.ServerSocket

import com.github.rerorero.reroft.grpc.test.TestEntry
import scalapb.GeneratedMessageCompanion

trait TestUtil extends ArbitrarySet {
  implicit val testEntryCompanion: GeneratedMessageCompanion[TestEntry] = TestEntry

  def unusedPorts(num: Int): Seq[Int] = {
    val sockets = (1 to num).map(_ => new ServerSocket(0))
    val ports = sockets.map(_.getLocalPort)
    sockets.foreach(_.close())
    ports
  }
}
