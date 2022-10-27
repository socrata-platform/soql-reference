package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.HasDoc

trait SelectListReferenceImpl[+CT] { this: SelectListReference[CT] =>
  type Self[+CT, +CV] = SelectListReference[CT]

  val size = 1

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    throw new Exception("Cannot ask for ColumnReferences on a query with SelectListReferences")

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean = {
    this == that
  }

  protected def doDebugDoc(implicit ev: HasDoc[Nothing]) =
    Doc(index).annotate(Annotation.SelectListReference(index))

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)

  def find(predicate: Expr[CT, Nothing] => Boolean): Option[Expr[CT, Nothing]] = Some(this).filter(predicate)
}

trait OSelectListReferenceImpl { this: SelectListReference.type =>
  implicit def serialize[CT : Writable] = new Writable[SelectListReference[CT]] {
    def writeTo(buffer: WriteBuffer, slr: SelectListReference[CT]): Unit = {
      buffer.write(slr.index)
      buffer.write(slr.isAggregated)
      buffer.write(slr.isWindowed)
      buffer.write(slr.typ)
      buffer.write(slr.position)
    }
  }

  implicit def deserialize[CT : Readable] = new Readable[SelectListReference[CT]] {
    def readFrom(buffer: ReadBuffer): SelectListReference[CT] = {
      SelectListReference(
        index = buffer.read[Int](),
        isAggregated = buffer.read[Boolean](),
        isWindowed = buffer.read[Boolean](),
        typ = buffer.read[CT]()
      )(
        buffer.read[Position]()
      )
    }
  }
}
