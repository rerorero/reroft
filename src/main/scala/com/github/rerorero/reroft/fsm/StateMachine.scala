package com.github.rerorero.reroft.fsm

import akka.actor.{Actor, ActorLogging, Props}

case class Apply(index: Long)
case object Initialize
// TODO: typed result and StateMachine
case class ApplyResult(computed: com.google.protobuf.any.Any, index: Long)

class StateMachine extends Actor with ActorLogging {
  override def receive: Receive = {
    case Apply(index) =>
      // TODO: implement my application
      log.info(s"apply async until ${index}")
      sender ! ApplyResult(null, index)
    case Initialize =>
      log.info(s"initialize")
  }
}

object StateMachine {
  def props() = Props(new StateMachine)
}

