package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.{HasDoc, HasType}

trait NullLiteralImpl[MT <: MetaTypes] { this: NullLiteral[MT] =>
  type Self[MT <: MetaTypes] = NullLiteral[MT]

  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[CV]) = d"NULL"

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))
}

trait ONullLiteralImpl { this: NullLiteral.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#CT]) = new Writable[NullLiteral[MT]] {
    def writeTo(buffer: WriteBuffer, nl: NullLiteral[MT]): Unit = {
      buffer.write(nl.typ)
      buffer.write(nl.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#CT]) = new Readable[NullLiteral[MT]] {
    def readFrom(buffer: ReadBuffer): NullLiteral[MT] = {
      NullLiteral(
        buffer.read[MT#CT]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
