package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

trait LiteralValueImpl[MT <: MetaTypes] { this: LiteralValue[MT] =>
  type Self[MT <: MetaTypes] = LiteralValue[MT]

  val size = 1

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) = ev.cv.docOf(value)

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    this.asInstanceOf[LiteralValue[MT2]] // SAFETY: this contains no column references
}

trait OLiteralValueImpl { this: LiteralValue.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCV: Writable[MT#ColumnValue]) = new Writable[LiteralValue[MT]] {
    def writeTo(buffer: WriteBuffer, lv: LiteralValue[MT]): Unit = {
      buffer.write(lv.value)
      buffer.write(lv.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCV: Readable[MT#ColumnValue], ht: HasType[MT#ColumnValue, MT#ColumnType]): Readable[LiteralValue[MT]] = new Readable[LiteralValue[MT]] with MetaTypeHelper[MT] {
    def readFrom(buffer: ReadBuffer): LiteralValue[MT] = {
      LiteralValue(
        buffer.read[CV]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
