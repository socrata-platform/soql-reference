package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

trait VirtualColumnImpl[MT <: MetaTypes] extends LabelUniverse[MT] { this: VirtualColumn[MT] =>
  type Self[MT <: MetaTypes] = VirtualColumn[MT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
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
    )(state.convert(position))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(table = state.convert(table), column = state.convert(column))(position)

  val size = 1

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).
      annotate(Annotation.ColumnRef[MT](table, column))

  private[analyzer2] def reReference(reference: Source): Self[MT] =
    copy()(position = position.reReference(reference))

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}

trait OVirtualColumnImpl { this: VirtualColumn.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#ColumnType], writableRNS: Writable[MT#ResourceNameScope]) = new Writable[VirtualColumn[MT]] {
    def writeTo(buffer: WriteBuffer, c: VirtualColumn[MT]): Unit = {
      buffer.write(c.table)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#ColumnType], readableRNS: Readable[MT#ResourceNameScope]): Readable[VirtualColumn[MT]] = new Readable[VirtualColumn[MT]] with LabelUniverse[MT] {
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
