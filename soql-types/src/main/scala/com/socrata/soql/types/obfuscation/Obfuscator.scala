package com.socrata.soql.types.obfuscation

abstract class Obfuscator(prefix: String, cryptProvider: CryptProvider) {
  private val formatter = LongFormatter
  private var _encryptor: CryptProvider.Transformer = null
  private var _decryptor: CryptProvider.Transformer = null

  def encryptor = {
    if(_encryptor == null) _encryptor = cryptProvider.encryptor
    _encryptor
  }

  private def decryptor = {
    if(_decryptor == null) _decryptor = cryptProvider.decryptor
    _decryptor
  }

  protected def encrypt(value: Long) =
    prefix + formatter.format(encryptor(value))

  protected def decrypt(value: String): Option[Long] =
    if(value.startsWith(prefix)) {
      formatter.deformat(value, prefix.length).map { x =>
        decryptor(x)
      }
    } else {
      None
    }
}

object Obfuscator {
  def isPossibleObfuscatedValue(x: String, prefix: String): Boolean =
    x.startsWith(prefix) && LongFormatter.isFormattedValue(x, prefix.length)
}
