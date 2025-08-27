package com.socrata.soql.types.obfuscation

import java.security.SecureRandom

class CryptProvider(keyMaterial: Array[Byte]) {
  private lazy val blowfish = Blowfish(keyMaterial)

  val encryptor =
    new CryptProvider.Transformer {
      def apply(in: Long): Long = {
        blowfish.encrypt(in)
      }
    }

  val decryptor =
    new CryptProvider.Transformer {
      def apply(in: Long): Long = {
        blowfish.decrypt(in)
      }
    }
}

object CryptProvider {
  private val rng = new SecureRandom

  def generateKey(): Array[Byte] = {
    val bs = new Array[Byte](72)
    rng.nextBytes(bs)
    bs
  }

  val zeros: CryptProvider = new CryptProvider(new Array[Byte](72))

  trait Transformer {
    def apply(in: Long): Long
  }
}

