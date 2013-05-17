package com.socrata.soql.types.obfuscation

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

class ObfuscatorTest extends FunSuite with MustMatchers with PropertyChecks {
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
}
