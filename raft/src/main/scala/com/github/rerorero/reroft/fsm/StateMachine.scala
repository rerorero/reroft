package com.github.rerorero.reroft.fsm

import com.google.protobuf.any.Any
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

case class Apply(index: Long)
case object Initialize
case class ApplyResult[Computed <: GeneratedMessage with Message[Computed]](computed: Computed, index: Long) {
  lazy val toAny: Any = Any.pack(computed)
}

object ApplyResult {
  def fromAny[Computed <: GeneratedMessage with Message[Computed]](any: Any, index: Long)(implicit com: GeneratedMessageCompanion[Computed]): ApplyResult[Computed] =
    ApplyResult(any.unpack[Computed], index)
}

// You can create your application on top of state machine actor like this
//
// class StateMachine[Computed <: GeneratedMessage with Message[Computed]] extends Actor with ActorLogging {
//   override def receive: Receive = {
//     case Apply(index) =>
//       log.info(s"apply logs until specified index and respond computed result to sender")
//       sender ! ApplyResult[Computed](<result>, index)
//     case Initialize =>
//       log.info(s"initialize this actor here")
//   }
// }

