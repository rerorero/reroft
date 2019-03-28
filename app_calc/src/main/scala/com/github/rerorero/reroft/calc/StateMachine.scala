package com.github.rerorero.reroft.calc

import akka.actor.{Actor, ActorLogging, Props}
import com.github.rerorero.reroft.fsm.{Apply, ApplyResult, Initialize}
import com.github.rerorero.reroft.grpc.calc.{CalcEntry, CalcResult, Command}

class StateMachine(logs: Logs) extends Actor with ActorLogging {
  var applyIndex = 0L
  var computed: Double = 0L

  def applyEntry(entry: CalcEntry): Unit =
    entry match {
      case CalcEntry(Command.ADD, cdr) =>
        computed += cdr
      case CalcEntry(Command.SUB, cdr) =>
        computed -= cdr
      case CalcEntry(Command.MULTI, cdr) =>
        computed *= cdr
      case CalcEntry(Command.DIV, cdr) if computed != 0 =>
        computed /= cdr
      case _ =>
        // TODO: handle error
        log.error(s"failed to apply log=${entry}, computed=${computed}")
    }

  override def receive: Receive = {
    case Apply(index) =>
      if (applyIndex < index) {
        logs.getLogs(applyIndex + 1, Some(index)).foreach(l => applyEntry(l.entry))
        applyIndex = index
      } else if (applyIndex > index) {
        // TODO: handle error
        log.error(s"discover decreasing index: apply=${applyIndex}, index=${index}")
      }

      sender ! ApplyResult[CalcResult](CalcResult(computed), applyIndex)

    case Initialize =>
      applyIndex = 0L
      computed = 0L
  }
}

object StateMachine {
  def props(logRepo: Logs) = Props(new StateMachine(logRepo))
}
