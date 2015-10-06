package com.socrata.soql.types.obfuscation

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.prop.PropertyChecks

class QuadifierTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Quadify always produces exactly four characters") {
    forAll { (x: Int) =>
      Quadifier.quadify(x).length must be (4)
    }
  }

  test("Quadify-into-array writes four characters at the specified offset") {
    val n = 100
    for(i <- 0 until n) {
      val buf = new Array[Char](n + 4)
      Quadifier.quadify(-1, buf, i)
      buf.take(i) must equal (Array.fill(i)('\u0000'))
      buf.drop(i).take(4) must equal (Array('z','z','z','z'))
      buf.drop(i + 4) must equal (Array.fill(n - i)('\u0000'))
    }
  }

  test("The result of quadification is always a quad") {
    forAll { (x: Int) =>
      Quadifier.isQuad(Quadifier.quadify(x)) must be (true)
    }
  }

  test("isQuad ignores prefixes and suffixes") {
    forAll { (x: Int, prefix: String, suffix: String) =>
      Quadifier.isQuad(prefix + Quadifier.quadify(x) + suffix, prefix.length) must be (true)
    }
  }

  test("Dequadify-quadify preserves the 20 least significant bits") {
    forAll { (x: Int) =>
      Quadifier.dequadifyEx(Quadifier.quadify(x)) must equal (x & 0xfffff)
    }
  }

  test("Dequadify ignores prefix and suffixes") {
    forAll { (x: Int, prefix: String, suffix: String) =>
      Quadifier.dequadifyEx(prefix + Quadifier.quadify(x) + suffix, prefix.length) must equal (x & 0xfffff)
    }
  }
}
