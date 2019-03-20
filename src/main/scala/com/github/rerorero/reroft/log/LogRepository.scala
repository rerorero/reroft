package com.github.rerorero.reroft.log
import com.github.rerorero.reroft.{LogEntry => GRPCLogEntry}

case class LogEntry()

object LogEntry {
  def fromMessage(m: GRPCLogEntry): LogEntry = {
    // TODO: convert
    LogEntry()
  }
}

trait LogRepository {
  def getCommitIndex(): Long
  def contains(term: Long, index: Long): Boolean
  def removeConflicted(term: Long, index: Long): Unit
  def append(entries: Seq[LogEntry]): Unit
  def commit(destIndex: Long): Unit
}

// TODO: remove
object logRepositoryDummy extends LogRepository {
  override def getCommitIndex(): Long = ???
  override def contains(term: Long, index: Long): Boolean = ???
  override def removeConflicted(term: Long, index: Long): Unit = ???
  override def append(entries: Seq[LogEntry]): Unit = ???
  override def commit(destIndex: Long): Unit = ???
}