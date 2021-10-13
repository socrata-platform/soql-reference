package com.socrata.soql.types.obfuscation

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ObfuscatorTest extends FunSuite with MustMatchers with ScalaCheckPropertyChecks {
  private class LongObfuscator(cryptProvider: CryptProvider, prefix: String) extends Obfuscator(prefix, cryptProvider) {
    def obfuscate(l: Long) = encrypt(l)
    def deobfuscate(s: String) = decrypt(s)
  }

  test("deobfuscate-of-obfuscate is the identity") {
    forAll { (x: Long, prefix: String, key: Array[Byte]) =>
      whenever(key.nonEmpty) {
        val cryptProvider = new CryptProvider(key)
        val obf = new LongObfuscator(cryptProvider, prefix)
        obf.deobfuscate(obf.obfuscate(x)) must equal (Some(x))
      }
    }
  }

  test("obfuscate starts with the given prefix") {
    forAll { (x: Long, prefix: String, key: Array[Byte]) =>
      whenever(key.nonEmpty) {
        val cryptProvider = new CryptProvider(key)
        val obf = new LongObfuscator(cryptProvider, prefix)
        obf.obfuscate(x).startsWith(prefix) must be (true)
      }
    }
  }
}
