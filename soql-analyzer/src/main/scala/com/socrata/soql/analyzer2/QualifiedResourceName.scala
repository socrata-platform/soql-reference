package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder}

import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.environment.ResourceName

case class QualifiedResourceName[+RNS](scope: RNS, name: ResourceName)

object QualifiedResourceName {
  implicit def encode[RNS: JsonEncode] = AutomaticJsonEncodeBuilder[QualifiedResourceName[RNS]]
  implicit def decode[RNS: JsonDecode] = AutomaticJsonDecodeBuilder[QualifiedResourceName[RNS]]

  implicit def serialize[RNS: Writable]: Writable[QualifiedResourceName[RNS]] = new Writable[QualifiedResourceName[RNS]] {
    def writeTo(buffer: WriteBuffer, qrn: QualifiedResourceName[RNS]): Unit = {
      buffer.write(qrn.scope)
      buffer.write(qrn.name)
    }
  }

  implicit def deserialize[RNS: Readable]: Readable[QualifiedResourceName[RNS]] = new Readable[QualifiedResourceName[RNS]] {
    def readFrom(buffer: ReadBuffer): QualifiedResourceName[RNS] = {
      QualifiedResourceName(
        scope = buffer.read[RNS](),
        name = buffer.read[ResourceName]()
      )
    }
  }
}
