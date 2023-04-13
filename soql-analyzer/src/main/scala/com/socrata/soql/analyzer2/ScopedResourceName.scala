package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder}

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.environment.ResourceName

case class ScopedResourceName[+RNS](scope: RNS, name: ResourceName)

object ScopedResourceName {
  implicit def encode[RNS: JsonEncode] = AutomaticJsonEncodeBuilder[ScopedResourceName[RNS]]
  implicit def decode[RNS: JsonDecode] = AutomaticJsonDecodeBuilder[ScopedResourceName[RNS]]

  implicit def serialize[RNS: Writable]: Writable[ScopedResourceName[RNS]] = new Writable[ScopedResourceName[RNS]] {
    def writeTo(buffer: WriteBuffer, srn: ScopedResourceName[RNS]): Unit = {
      buffer.write(srn.scope)
      buffer.write(srn.name)
    }
  }

  implicit def deserialize[RNS: Readable]: Readable[ScopedResourceName[RNS]] = new Readable[ScopedResourceName[RNS]] {
    def readFrom(buffer: ReadBuffer): ScopedResourceName[RNS] = {
      ScopedResourceName(
        scope = buffer.read[RNS](),
        name = buffer.read[ResourceName]()
      )
    }
  }
}
