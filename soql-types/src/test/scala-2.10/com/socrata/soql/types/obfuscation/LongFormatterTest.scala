package com.socrata.soql.types.obfuscation

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

class LongFormatterTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Format always produces exactly 14 characters") {
    forAll { x: Long =>
      LongFormatter.format(x).length must be (14)
    }
  }

  test("Deformat-format is the identity") {
    forAll { (x: Long) =>
      LongFormatter.deformatEx(LongFormatter.format(x)) must equal (x)
    }
  }

  test("Deformat ignores declared prefixes") {
    forAll { (x: Long, prefix: String) =>
      LongFormatter.deformatEx(prefix + LongFormatter.format(x), prefix.length) must equal (x)
    }
  }

  test("Deformat rejects suffixes") {
    forAll { (x: Long, suffix: String) =>
      whenever(suffix != "") {
        evaluating {
          LongFormatter.deformatEx(LongFormatter.format(x) + suffix) must equal (x)
        } must produce[IllegalArgumentException]
      }
    }
  }

  test("Format always produces a formatted value") {
    forAll { (x: Long) =>
      LongFormatter.isFormattedValue(LongFormatter.format(x)) must be (true)
    }
  }

  test("isFormattedValue ignores declared prefixes") {
    forAll { (x: Long, prefix: String) =>
      LongFormatter.isFormattedValue(prefix + LongFormatter.format(x), prefix.length) must be (true)
    }
  }

  test("isFormattedValue rejects suffixes") {
    forAll { (x: Long, suffix: String) =>
      whenever(suffix != "") {
        LongFormatter.isFormattedValue(LongFormatter.format(x) + suffix) must be (false)
      }
    }
  }
}
