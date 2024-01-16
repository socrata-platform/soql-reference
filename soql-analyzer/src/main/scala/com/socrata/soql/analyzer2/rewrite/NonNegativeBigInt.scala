package com.socrata.soql.analyzer2.rewrite

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}

import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer, Readable, Writable}

class NonNegativeBigInt private (val underlying: BigInt) {
  def +(that: NonNegativeBigInt) = new NonNegativeBigInt(this.underlying + that.underlying)
  def *(that: NonNegativeBigInt) = new NonNegativeBigInt(this.underlying * that.underlying)
}

object NonNegativeBigInt {
  private val zero = BigInt(0)

  implicit object jcodec extends JsonEncode[NonNegativeBigInt] with JsonDecode[NonNegativeBigInt] {
    override def encode(x: NonNegativeBigInt) = JsonEncode.toJValue(x.underlying)
    override def decode(v: JValue) =
      JsonDecode.fromJValue[BigInt](v).flatMap { b =>
        if(b >= zero) Right(new NonNegativeBigInt(b))
        else Left(DecodeError.InvalidValue(v))
      }
  }

  implicit object serialize extends Readable[NonNegativeBigInt] with Writable[NonNegativeBigInt] {
    override def readFrom(buf: ReadBuffer): NonNegativeBigInt = {
      val b = buf.read[BigInt]()
      if(b >= zero) new NonNegativeBigInt(b)
      else fail("Found negative bigint")
    }
    override def writeTo(buf: WriteBuffer, bi: NonNegativeBigInt): Unit =
      buf.write(bi.underlying)
  }

  def apply(bi: BigInt): Option[NonNegativeBigInt] =
    if(bi >= zero) Some(new NonNegativeBigInt(bi))
    else None
}
