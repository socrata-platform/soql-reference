package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.HasDoc

trait VirtualColumnImpl[MT <: MetaTypes] extends LabelHelper[MT] { this: VirtualColumn[MT] =>
  type Self[MT <: MetaTypes] = Column[MT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map(table -> Set(column))

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = {
    that match {
      case VirtualColumn(thatTable, thatColumn, thatTyp) =>
        this.typ == that.typ &&
          state.tryAssociate(Some(this.table), this.column, Some(thatTable), thatColumn)
      case _ =>
        false
    }
  }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    VirtualColumn(
      table = table,
      column = column,
      typ = state.changesOnlyLabels.convertCT(typ)
    )(position)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(table = state.convert(table), column = state.convert(column))(position)

  val size = 1

  def doDebugDoc(implicit ev: HasDoc[CV]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).
      annotate(Annotation.ColumnRef[MT](table, column))

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}

trait OVirtualColumnImpl { this: VirtualColumn.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#CT]) = new Writable[VirtualColumn[MT]] {
    def writeTo(buffer: WriteBuffer, c: VirtualColumn[MT]): Unit = {
      buffer.write(c.table)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#CT]): Readable[VirtualColumn[MT]] = new Readable[VirtualColumn[MT]] with MetaTypeHelper[MT] with LabelHelper[MT] {
    def readFrom(buffer: ReadBuffer): VirtualColumn[MT] = {
      VirtualColumn(
        table = buffer.read[AutoTableLabel](),
        column = buffer.read[AutoColumnLabel](),
        typ = buffer.read[CT]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
