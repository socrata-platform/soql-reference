package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.{HasDoc, HasType}

trait NullLiteralImpl[+CT] { this: NullLiteral[CT] =>
  type Self[+CT, +CV] = NullLiteral[CT]

  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[Nothing]) = d"NULL"

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)
}

trait ONullLiteralImpl { this: NullLiteral.type =>
  implicit def serialize[CT: Writable] = new Writable[NullLiteral[CT]] {
    def writeTo(buffer: WriteBuffer, nl: NullLiteral[CT]): Unit = {
      buffer.write(nl.typ)
      buffer.write(nl.position)
    }
  }

  implicit def deserialize[CT: Readable] = new Readable[NullLiteral[CT]] {
    def readFrom(buffer: ReadBuffer): NullLiteral[CT] = {
      NullLiteral(
        buffer.read[CT]()
      )(
        buffer.read[Position]()
      )
    }
  }
}
