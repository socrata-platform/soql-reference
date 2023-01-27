package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.typechecker.HasDoc

trait PhysicalColumnImpl[MT <: MetaTypes] extends LabelHelper[MT] { this: PhysicalColumn[MT] =>
  type Self[MT <: MetaTypes] = Column[MT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map(table -> Set(column))

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = {
    that match {
      case PhysicalColumn(thatPhysicalTable, thatTable, thatColumn, thatTyp) =>
        this.typ == that.typ &&
          this.physicalTable == thatPhysicalTable &&
          state.tryAssociate(Some(this.table), this.column, Some(thatTable), thatColumn)
      case _ =>
        false
    }
  }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    PhysicalColumn(
      physicalTable = state.convert(physicalTable),
      table = table,
      column = state.convert(physicalTable, column),
      typ = state.changesOnlyLabels.convertCT(typ)
    )(position)

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  val size = 1

  def doDebugDoc(implicit ev: HasDoc[CV]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).
      annotate(Annotation.ColumnRef(table, column))

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}

trait OPhysicalColumnImpl { this: PhysicalColumn.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#CT], writableDTN : Writable[MT#DatabaseTableNameImpl], writableDCN : Writable[MT#DatabaseColumnNameImpl]): Writable[PhysicalColumn[MT]] = new Writable[PhysicalColumn[MT]] {
    def writeTo(buffer: WriteBuffer, c: PhysicalColumn[MT]): Unit = {
      buffer.write(c.physicalTable)
      buffer.write(c.table)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#CT], readableDTN : Readable[MT#DatabaseTableNameImpl], readableDCN : Readable[MT#DatabaseColumnNameImpl]): Readable[PhysicalColumn[MT]] = new Readable[PhysicalColumn[MT]] with MetaTypeHelper[MT] with LabelHelper[MT] {
    def readFrom(buffer: ReadBuffer): PhysicalColumn[MT] = {
      PhysicalColumn(
        physicalTable = buffer.read[DatabaseTableName](),
        table = buffer.read[AutoTableLabel](),
        column = buffer.read[DatabaseColumnName](),
        typ = buffer.read[CT]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
