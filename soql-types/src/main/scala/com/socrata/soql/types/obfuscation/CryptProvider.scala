package com.socrata.soql.types.obfuscation

import java.security.SecureRandom

class CryptProvider(keyMaterial: Array[Byte]) {
  def key = java.util.Arrays.copyOf(keyMaterial, keyMaterial.length)
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

    private def extractLong(bs: Array[Byte], off: Int): Long = {
      var result = 0
      var i = off
      while(i != off + 8) {
        result <<= 8
        result += bs(i) & 0xff
        i += 1
      }
      java.lang.Long.reverseBytes(result)
    }

    private def injectLong(bs: Array[Byte], off: Int, n: Long) = {
      var nv = n
      var i = off
      while(i != off + 8) {
        bs(i) = (nv & 0xff).toByte
        nv >>>= 8
        i += 1
      }
    }

    final def processBlock(inBuf: Array[Byte], inOff: Int, outBuf: Array[Byte], outOff: Int): Unit = {
      injectLong(outBuf, outOff, apply(extractLong(inBuf, inOff)))
    }
  }
}

