package com.socrata.soql.types.obfuscation

import org.bouncycastle.crypto.engines.BlowfishEngine

abstract class Obfuscator(prefix: String, cryptProvider: CryptProvider) {
  private val formatter = LongFormatter
  private var _encryptor: BlowfishEngine = null
  private var _decryptor: BlowfishEngine = null

  def encryptor = {
    if(_encryptor == null) _encryptor = cryptProvider.encryptor
    _encryptor
  }

  private def decryptor = {
    if(_decryptor == null) _decryptor = cryptProvider.decryptor
    _decryptor
  }

  private def byteify(bs: Array[Byte], x: Long) {
    bs(0) = x.toByte
    bs(1) = (x >> 8).toByte
    bs(2) = (x >> 16).toByte
    bs(3) = (x >> 24).toByte
    bs(4) = (x >> 32).toByte
    bs(5) = (x >> 40).toByte
    bs(6) = (x >> 48).toByte
    bs(7) = (x >> 56).toByte
  }

  private def debyteify(bs: Array[Byte]): Long =
    (bs(0) & 0xff).toLong +
      ((bs(1) & 0xff).toLong << 8) +
      ((bs(2) & 0xff).toLong << 16) +
      ((bs(3) & 0xff).toLong << 24) +
      ((bs(4) & 0xff).toLong << 32) +
      ((bs(5) & 0xff).toLong << 40) +
      ((bs(6) & 0xff).toLong << 48) +
      ((bs(7) & 0xff).toLong << 56)

  private def crypt(x: Long, engine: BlowfishEngine): Long = {
    val buf = new Array[Byte](8)
    byteify(buf, x)
    // as it happens, I know passing in buf twice is safe
    // because the the implementation only uses it to extract
    // a pair of ints at the start and fill it back in with
    // a pair of ints at the end.
    // I wish I could just pass those two ints directly instead
    // of allocating a temp buffer.
    engine.processBlock(buf, 0, buf, 0)
    debyteify(buf)
  }

  protected def encrypt(value: Long) =
    prefix + formatter.format(crypt(value, encryptor))

  protected def decrypt(value: String): Option[Long] =
    if(value.startsWith(prefix)) {
      formatter.deformat(value, prefix.length).map { x =>
        crypt(x, decryptor)
      }
    } else {
      None
    }
}

object Obfuscator {
  def isPossibleObfuscatedValue(x: String, prefix: String): Boolean =
    x.startsWith(prefix) && LongFormatter.isFormattedValue(x, prefix.length)
}
