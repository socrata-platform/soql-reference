package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

trait NullLiteralImpl[MT <: MetaTypes] { this: NullLiteral[MT] =>
  type Self[MT <: MetaTypes] = NullLiteral[MT]

  val size = 1

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) = d"NULL"

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    this.asInstanceOf[NullLiteral[MT2]] // SAFETY: This does not contain any labels

  private[analyzer2] def reReference(reference: Source): Self[MT] =
    copy()(position = position.reReference(reference))
}

trait ONullLiteralImpl { this: NullLiteral.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#ColumnType], writableRNS: Writable[MT#ResourceNameScope]) = new Writable[NullLiteral[MT]] {
    def writeTo(buffer: WriteBuffer, nl: NullLiteral[MT]): Unit = {
      buffer.write(nl.typ)
      buffer.write(nl.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#ColumnType], readableRNS: Readable[MT#ResourceNameScope]) = new Readable[NullLiteral[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): NullLiteral[MT] = {
      NullLiteral(
        buffer.read[MT#ColumnType]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
