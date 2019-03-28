package com.github.rerorero.reroft.fsm

import com.google.protobuf.any.Any
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

case class Apply(indexTo: Long)
case object Initialize
case class ApplyResult[Computed <: GeneratedMessage with Message[Computed]](computed: Computed, index: Long) {
  lazy val toAny: Any = Any.pack(computed)
}

object ApplyResult {
  def fromAny[Computed <: GeneratedMessage with Message[Computed]](any: Any, index: Long)(implicit com: GeneratedMessageCompanion[Computed]): ApplyResult[Computed] =
    ApplyResult(any.unpack[Computed], index)
}
