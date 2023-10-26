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
  def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) = {
    implicit val newHasType = hasType.asInstanceOf[HasType[types.ColumnValue[MT2], types.ColumnType[MT2]]] // SAFETY: rewriting database names does not change types
    LiteralValue[MT2](state.rewriteProvenance(value))(state.convert(this.position))
  }
}

trait OLiteralValueImpl { this: LiteralValue.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCV: Writable[MT#ColumnValue], writableRNS: Writable[MT#ResourceNameScope]) = new Writable[LiteralValue[MT]] {
    def writeTo(buffer: WriteBuffer, lv: LiteralValue[MT]): Unit = {
      buffer.write(lv.value)
      buffer.write(lv.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCV: Readable[MT#ColumnValue], ht: HasType[MT#ColumnValue, MT#ColumnType], rns: Readable[MT#ResourceNameScope]): Readable[LiteralValue[MT]] = new Readable[LiteralValue[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): LiteralValue[MT] = {
      LiteralValue(
        buffer.read[CV]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
