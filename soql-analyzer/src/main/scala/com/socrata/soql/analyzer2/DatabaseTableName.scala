package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{WrapperJsonEncode, WrapperJsonDecode}

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2.serialization.{ReadBuffer, WriteBuffer, Readable, Writable}
import com.socrata.soql.typechecker.HasDoc

final case class DatabaseTableName[+T](name: T) {
  def debugDoc(implicit ev: HasDoc[T]): Doc[Nothing] = ev.docOf(name)
}

object DatabaseTableName {
  implicit def jEncode[T: JsonEncode] = WrapperJsonEncode[DatabaseTableName[T]](_.name)
  implicit def jDecode[T: JsonDecode] = WrapperJsonDecode[DatabaseTableName[T]](DatabaseTableName[T](_))

  implicit def serialize[T: Writable] = new Writable[DatabaseTableName[T]] {
    def writeTo(buffer: WriteBuffer, d: DatabaseTableName[T]) =
      buffer.write(d.name)
  }

  implicit def deserialize[T: Readable] = new Readable[DatabaseTableName[T]] {
    def readFrom(buffer: ReadBuffer) =
      DatabaseTableName(buffer.read[T]())
  }
}
