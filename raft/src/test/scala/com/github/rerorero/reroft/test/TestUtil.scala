package com.github.rerorero.reroft.test

import com.github.rerorero.reroft.grpc.test.TestEntry
import scalapb.GeneratedMessageCompanion

trait TestUtil extends ArbitrarySet {
  implicit val testEntryCompanion: GeneratedMessageCompanion[TestEntry] = TestEntry
}
