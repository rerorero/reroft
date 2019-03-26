package com.github.rerorero.reroft.log
import com.github.rerorero.reroft.LogEntry
import com.github.rerorero.reroft.test.TestEntry
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
  def removeConflicted(term: Long, index: Long): Unit
  def append(entries: Seq[LogRepoEntry[Entry]]): Unit
  def commit(destIndex: Long): Unit
  def getLogs(fromIndex: Long): Seq[LogRepoEntry[Entry]]
}

// TODO: remove
object logRepositoryDummy extends LogRepository[TestEntry] {
  override def getCommitIndex(): Long = ???
  override def contains(term: Long, index: Long): Boolean = ???
  override def removeConflicted(term: Long, index: Long): Unit = ???
  override def append(entries: Seq[LogRepoEntry[TestEntry]]): Unit = ???
  override def commit(destIndex: Long): Unit = ???
  override def lastLogTerm(): Long = ???
  override def lastLogIndex(): Long = ???
  override def empty(): Unit = ???
  override def getLogs(fromIndex: Long): Seq[LogRepoEntry[TestEntry]] = ???
}