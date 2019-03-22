package com.github.rerorero.reroft.fsm

import akka.actor.{Actor, ActorLogging, Props}

case class ApplyAsync(index: Long)
case class ApplySync(index: Long)
case object Initialize

class StateMachine extends Actor with ActorLogging {
  override def receive: Receive = {
    case ApplyAsync(index) =>
      // TODO: implement my application
      log.info(s"apply async until ${index}")
    case ApplySync(index) =>
      log.info(s"apply sync until ${index}")
    case Initialize =>
      log.info(s"initialize")
  }
}

object StateMachine {
  def props() = Props(new StateMachine)
}

