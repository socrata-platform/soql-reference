package com.socrata.soql.types.obfuscation

import java.nio.charset.StandardCharsets

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

  test("obfuscate gives a result consistent with previous versions") {
    val key = "1234567890123456789012345678901234567890123456789012345678900123456789012".getBytes(StandardCharsets.UTF_8)
    val cryptProvider = new CryptProvider(key)
    val obf = new LongObfuscator(cryptProvider, "x-")
    obf.obfuscate(0x123456789abcdefL) must be ("x-3kj7~zdra_5xia")
    obf.deobfuscate("x-3kj7~zdra_5xia") must be (Some(0x123456789abcdefL))
  }
}
