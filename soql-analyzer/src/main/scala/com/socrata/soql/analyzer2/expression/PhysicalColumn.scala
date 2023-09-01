package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

trait PhysicalColumnImpl[MT <: MetaTypes] extends LabelUniverse[MT] { this: PhysicalColumn[MT] =>
  type Self[MT <: MetaTypes] = PhysicalColumn[MT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    Map(table -> Set(column))

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = {
    that match {
      case PhysicalColumn(thatTable, thatTableName, thatTableCanonicalName, thatColumn, thatTyp) =>
        this.typ == that.typ &&
          this.tableCanonicalName == thatTableCanonicalName &&
          state.physicalTableLeft(this.table) == state.physicalTableRight(thatTable) &&
          state.tryAssociate(Some(this.table), this.column, Some(thatTable), thatColumn)
      case _ =>
        false
    }
  }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    PhysicalColumn(
      table = table,
      tableName = state.convert(tableName),
      tableCanonicalName = tableCanonicalName,
      column = state.convert(table, column),
      typ = state.changesOnlyLabels.convertCT(typ)
    )(position)

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  val size = 1

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) =
    (table.debugDoc ++ d"." ++ column.debugDoc(ev.columnNameImpl)).
      annotate(Annotation.ColumnRef(table, column))

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}

trait OPhysicalColumnImpl { this: PhysicalColumn.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#ColumnType], writableDTN : Writable[MT#DatabaseTableNameImpl], writableDCN : Writable[MT#DatabaseColumnNameImpl]): Writable[PhysicalColumn[MT]] = new Writable[PhysicalColumn[MT]] {
    def writeTo(buffer: WriteBuffer, c: PhysicalColumn[MT]): Unit = {
      buffer.write(c.table)
      buffer.write(c.tableName)
      buffer.write(c.tableCanonicalName)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#ColumnType], readableDTN : Readable[MT#DatabaseTableNameImpl], readableDCN : Readable[MT#DatabaseColumnNameImpl]): Readable[PhysicalColumn[MT]] = new Readable[PhysicalColumn[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): PhysicalColumn[MT] = {
      PhysicalColumn(
        table = buffer.read[AutoTableLabel](),
        tableName = buffer.read[DatabaseTableName](),
        tableCanonicalName = buffer.read[CanonicalName](),
        column = buffer.read[DatabaseColumnName](),
        typ = buffer.read[CT]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
