package com.github.rerorero.reroft.log
import com.github.rerorero.reroft.LogEntry

case class LogRepoEntry(
  term: Long,
  index: Long,
  // TODO: to be type safety by using T
  entry: com.google.protobuf.any.Any,
) {
  def toMessage: LogEntry = LogEntry(term, index, Some(entry))
}

object LogRepoEntry {
  def fromMessage(m: LogEntry): LogRepoEntry = LogRepoEntry(
    term = m.term,
    index = m.index,
    entry = m.entry.getOrElse(null),
  )
}

trait LogRepository {
  // TODO: handle errors
  def empty(): Unit
  def getCommitIndex(): Long
  def lastLogTerm(): Long
  def lastLogIndex(): Long
  def contains(term: Long, index: Long): Boolean
  def removeConflicted(term: Long, index: Long): Unit
  def append(entries: Seq[LogRepoEntry]): Unit
  def commit(destIndex: Long): Unit
  def getLogs(fromIndex: Long): Seq[LogRepoEntry]
}

// TODO: remove
object logRepositoryDummy extends LogRepository {
  override def getCommitIndex(): Long = ???
  override def contains(term: Long, index: Long): Boolean = ???
  override def removeConflicted(term: Long, index: Long): Unit = ???
  override def append(entries: Seq[LogRepoEntry]): Unit = ???
  override def commit(destIndex: Long): Unit = ???
  override def lastLogTerm(): Long = ???
  override def lastLogIndex(): Long = ???
  override def empty(): Unit = ???
  override def getLogs(fromIndex: Long): Seq[LogRepoEntry] = ???
}