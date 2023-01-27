package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.HasDoc

trait ColumnImpl[MT <: MetaTypes] extends LabelHelper[MT] { this: Column[MT] =>
  type Self[MT <: MetaTypes] = Column[MT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map(table -> Set(column))

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = {
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

  def doDebugDoc(implicit ev: HasDoc[CV]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).
      annotate(Annotation.ColumnRef(table, column))

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}

trait OColumnImpl { this: Column.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#CT], writableDTN : Writable[MT#DatabaseTableNameImpl], writableDCN : Writable[MT#DatabaseColumnNameImpl]): Writable[Column[MT]] = new Writable[Column[MT]] {
    def writeTo(buffer: WriteBuffer, c: Column[MT]): Unit = {
      buffer.write(c.table)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#CT], readableDTN : Readable[MT#DatabaseTableNameImpl], readableDCN : Readable[MT#DatabaseColumnNameImpl]): Readable[Column[MT]] = new Readable[Column[MT]] with MetaTypeHelper[MT] with LabelHelper[MT] {
    def readFrom(buffer: ReadBuffer): Column[MT] = {
      Column(
        table = buffer.read[TableLabel](),
        column = buffer.read[ColumnLabel](),
        typ = buffer.read[CT]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
