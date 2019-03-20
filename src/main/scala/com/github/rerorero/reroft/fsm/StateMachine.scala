package com.github.rerorero.reroft.fsm

import akka.actor.{Actor, ActorLogging, LoggingFSM, Props}

// Applier needn't concern thread safety
trait Applier {
  def apply(commitIndex: Long): Unit
}

case class Apply(index: Long)

class StateMachine(applier: Applier) extends Actor with ActorLogging {
  override def receive: Receive = {
    case Apply(index) =>
      applier.apply(index)
      log.info(s"commietted to ${index}")
  }
}

object StateMachine {
  def props(applier: Applier) = Props(new StateMachine(applier))
}

// TODO: remove
object applierDummy extends Applier {
  override def apply(commitIndex: Long): Unit = ???
}