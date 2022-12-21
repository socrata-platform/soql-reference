package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.{HasDoc, HasType}

trait LiteralValueImpl[+CT, +CV] { this: LiteralValue[CT, CV] =>
  type Self[+CT, +CV] = LiteralValue[CT, CV]

  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[CV]) = ev.docOf(value)

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
}

trait OLiteralValueImpl { this: LiteralValue.type =>
  implicit def serialize[CT, CV: Writable] = new Writable[LiteralValue[CT, CV]] {
    def writeTo(buffer: WriteBuffer, lv: LiteralValue[CT, CV]): Unit = {
      buffer.write(lv.value)
      buffer.write(lv.position)
    }
  }

  implicit def deserialize[CT, CV: Readable](implicit ev: HasType[CV, CT]) = new Readable[LiteralValue[CT, CV]] {
    def readFrom(buffer: ReadBuffer): LiteralValue[CT, CV] = {
      LiteralValue(
        buffer.read[CV]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
