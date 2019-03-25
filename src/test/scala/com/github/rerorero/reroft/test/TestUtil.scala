package com.github.rerorero.reroft.test

import scalapb.GeneratedMessageCompanion

trait TestUtil extends ArbitrarySet {
  implicit val testEntryCompanion: GeneratedMessageCompanion[TestEntry] = TestEntry
}
