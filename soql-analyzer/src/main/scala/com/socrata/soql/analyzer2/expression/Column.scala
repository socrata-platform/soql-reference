package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.HasDoc

trait ColumnImpl[+CT] { this: Column[CT] =>
  type Self[+CT, +CV] = Column[CT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map(table -> Set(column))

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean = {
    that match {
      case Column(thatTable, thatColumn, thatTyp) =>
        this.typ == that.typ &&
          state.tryAssociate(Some(this.table), this.column, Some(thatTable), thatColumn)
      case _ =>
        false
    }
  }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    column match {
      case dcn: DatabaseColumnName =>
        copy(column = state.convert(table, dcn))(position)
      case _ =>
        this
    }

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(table = state.convert(table), column = state.convert(column))(position)

  val size = 1

  def doDebugDoc(implicit ev: HasDoc[Nothing]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).
      annotate(Annotation.ColumnRef(table, column))

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)

  def find(predicate: Expr[CT, Nothing] => Boolean): Option[Expr[CT, Nothing]] = Some(this).filter(predicate)
}

trait OColumnImpl { this: Column.type =>
  implicit def serialize[CT : Writable] = new Writable[Column[CT]] {
    def writeTo(buffer: WriteBuffer, c: Column[CT]): Unit = {
      buffer.write(c.table)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  implicit def deserialize[CT : Readable] = new Readable[Column[CT]] {
    def readFrom(buffer: ReadBuffer): Column[CT] = {
      Column(
        table = buffer.read[TableLabel](),
        column = buffer.read[ColumnLabel](),
        typ = buffer.read[CT]()
      )(
        buffer.read[Position]()
      )
    }
  }
}
