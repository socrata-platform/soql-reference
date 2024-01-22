package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, FieldEncode, FieldDecode}
import com.rojoma.json.v3.util.{WrapperJsonEncode, WrapperJsonDecode, WrapperFieldEncode}

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer, Readable, Writable}

final case class DatabaseTableName[+T](name: T) {
  def debugDoc(implicit ev: HasDoc[T]): Doc[Nothing] = ev.docOf(name)
}

object DatabaseTableName {
  implicit def jEncode[T: JsonEncode] = WrapperJsonEncode[DatabaseTableName[T]](_.name)
  implicit def jDecode[T: JsonDecode] = WrapperJsonDecode[DatabaseTableName[T]](DatabaseTableName[T](_))

  implicit def fEncode[T: FieldEncode] = WrapperFieldEncode[DatabaseTableName[T]]({ dtn => FieldEncode[T].encode(dtn.name) })
  implicit def fDecode[T: FieldDecode] = new FieldDecode[DatabaseTableName[T]] {
    override def decode(s: String) = FieldDecode[T].decode(s).map(DatabaseTableName(_))
  }

  implicit def serialize[T: Writable] = new Writable[DatabaseTableName[T]] {
    def writeTo(buffer: WriteBuffer, d: DatabaseTableName[T]) =
      buffer.write(d.name)
  }

  implicit def deserialize[T: Readable] = new Readable[DatabaseTableName[T]] {
    def readFrom(buffer: ReadBuffer) =
      DatabaseTableName(buffer.read[T]())
  }

  implicit def ordering[T](implicit ordering: Ordering[T]): Ordering[DatabaseTableName[T]] =
    new Ordering[DatabaseTableName[T]] {
      def compare(a: DatabaseTableName[T], b: DatabaseTableName[T]) =
        ordering.compare(a.name, b.name)
    }
}
