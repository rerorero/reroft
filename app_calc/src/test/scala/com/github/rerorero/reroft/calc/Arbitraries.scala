package com.github.rerorero.reroft.calc

import com.github.rerorero.reroft.grpc.calc.{CalcEntry, Command}
import org.scalacheck.{Arbitrary, Gen}

trait CalcArbitraries {
  implicit val arbCalcEntry: Arbitrary[CalcEntry] = Arbitrary {
    for {
      command <- Gen.oneOf(Command.values.filterNot(_ == Command.COMMAND_UNKNOWN))
      cdr <- Gen.choose(-100000.0, 100000.0)
    } yield CalcEntry(command, cdr)
  }
}
