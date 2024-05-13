package com.socrata.soql.types.obfuscation

import java.security.SecureRandom

import org.bouncycastle.crypto.params.KeyParameter
import org.bouncycastle.crypto.engines.BlowfishEngine

class CryptProvider(keyMaterial: Array[Byte]) {
  def key = java.util.Arrays.copyOf(keyMaterial, keyMaterial.length)
  private val keyParam = new KeyParameter(keyMaterial)

  lazy val encryptor = locally {
    val bf = new BlowfishEngine
    bf.init(true, keyParam)
    bf
  }

  lazy val decryptor = locally {
    val bf = new BlowfishEngine
    bf.init(false, keyParam)
    bf
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
}

