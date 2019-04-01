package com.github.rerorero.reroft.calc

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.event.Logging
import com.github.rerorero.reroft.grpc.calc.{CalcEntry, Command}
import com.github.rerorero.reroft.logs.{LogRepoEntry, LogRepository}

import scala.collection.mutable.ListBuffer

class Logs(implicit system: ActorSystem) extends LogRepository[CalcEntry] {
  // You don't have to worry about thread safety
  val entries: ListBuffer[LogRepoEntry[CalcEntry]] = Logs.empty(new ListBuffer[LogRepoEntry[CalcEntry]])
  var commitIndex = new AtomicLong(0L)
  val log = Logging.getLogger(system, this)

  override def empty(): Unit = synchronized {
    Logs.empty(entries)
    commitIndex.set(0L)
  }

  override def getCommitIndex(): Long = commitIndex.get()

  override def lastLogTerm(): Long = entries.last.term

  override def lastLogIndex(): Long = entries.last.index

  override def contains(term: Long, index: Long): Boolean =
    entries.exists(e => term == e.term && index == e.index)

  override def removeConflicted(prevLogTerm: Long, prevLogIndex: Long): Unit = {
    val last = entries.last
    if (last.index == prevLogIndex && last.term == prevLogTerm) {
      // do nothing
    } else {
      entries.indexWhere(e => e.index == prevLogIndex && e.term == prevLogTerm) match {
        case index if index >= 0 =>
          synchronized(entries.remove(index + 1, entries.length - index - 1))
        case _ =>
          // TODO: handle error
          log.error(s"index=${prevLogIndex} not found")
      }
    }
  }


  override def append(newEntries: Seq[LogRepoEntry[CalcEntry]]): Unit =
    synchronized(entries ++= newEntries)

  override def commit(destIndex: Long): Unit = commitIndex.set(destIndex)

  override def getLogs(indexFrom: Long, indexTo: Option[Long]): Seq[LogRepoEntry[CalcEntry]] =
    (entries.indexWhere(_.index == indexFrom), indexTo.map(i => entries.indexWhere(_.index == i))) match {
      case (from, Some(to)) if from >= 0 && to >= 0 =>
        entries.slice(from, to + 1)
      case (from, None) if from >= 0 =>
        entries.drop(from)
      case (from, to) =>
        // out of range
        log.debug(s"unknown range: ${indexFrom} - ${indexTo}, from=${from} to=${to}")
        Seq.empty
    }
}

object Logs {
  val origin = LogRepoEntry(term = 0L, index = 0L, CalcEntry(Command.ADD, 0))
  def empty(list: ListBuffer[LogRepoEntry[CalcEntry]]) = {
    list.clear()
    list += origin
    list
  }
}
