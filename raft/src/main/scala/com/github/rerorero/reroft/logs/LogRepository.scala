package com.github.rerorero.reroft.logs

import com.github.rerorero.reroft.grpc.LogEntry
import com.google.protobuf.any.Any
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

case class LogRepoEntry[Entry <: GeneratedMessage with Message[Entry]](
  term: Long,
  index: Long,
  entry: Entry
) {
  lazy val toMessage: LogEntry = LogEntry(term, index, Some(Any.pack[Entry](entry)))
}

object LogRepoEntry {
  def fromMessage[T <: GeneratedMessage with Message[T]](msg: LogEntry)(implicit cmp: GeneratedMessageCompanion[T]): LogRepoEntry[T] =
    msg.entry match {
      case Some(entry) => LogRepoEntry(msg.term, msg.index, entry.unpack[T])
      case None => throw new Exception(s"log doesn't have entry: index=${msg.index}, term=${msg.term}")
    }
}

trait LogRepository[Entry <: GeneratedMessage with Message[Entry]] {
  // TODO: handle errors
  def empty(): Unit
  def getCommitIndex(): Long
  def lastLogTerm(): Long
  def lastLogIndex(): Long
  def contains(term: Long, index: Long): Boolean
  // removeConflicted removes logs which have term and index after than prevLogTerm and prevLogIndex
  def removeConflicted(prevLogTerm: Long, prevLogIndex: Long): Unit
  def append(entries: Seq[LogRepoEntry[Entry]]): Unit
  def commit(destIndex: Long): Unit
  def getLogs(indexFrom: Long, indexTo: Option[Long] = None): Seq[LogRepoEntry[Entry]]
}

