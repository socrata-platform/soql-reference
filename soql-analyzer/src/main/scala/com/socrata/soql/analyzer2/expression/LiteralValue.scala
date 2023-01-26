package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.{HasDoc, HasType}

trait LiteralValueImpl[MT <: MetaTypes] { this: LiteralValue[MT] =>
  type Self[MT <: MetaTypes] = LiteralValue[MT]

  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[CV]) = ev.docOf(value)

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
}

trait OLiteralValueImpl { this: LiteralValue.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCV: Writable[MT#CV]) = new Writable[LiteralValue[MT]] {
    def writeTo(buffer: WriteBuffer, lv: LiteralValue[MT]): Unit = {
      buffer.write(lv.value)
      buffer.write(lv.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCV: Readable[MT#CV], ht: HasType[MT#CV, MT#CT]) = new Readable[LiteralValue[MT]] {
    def readFrom(buffer: ReadBuffer): LiteralValue[MT] = {
      LiteralValue(
        buffer.read[MT#CV]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
