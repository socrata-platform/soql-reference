package com.socrata.soql.analyzer2

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

case class CanonicalName(name: String)

object CanonicalName extends (String => CanonicalName) {
  implicit val jCodec = WrapperJsonCodec[CanonicalName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[CanonicalName](this, _.name)

  implicit object serialize extends Readable[CanonicalName] with Writable[CanonicalName] {
    def writeTo(buffer: WriteBuffer, cn: CanonicalName): Unit = {
      buffer.write(cn.name)
    }

    def readFrom(buffer: ReadBuffer): CanonicalName = {
      CanonicalName(buffer.read[String]())
    }
  }
}
