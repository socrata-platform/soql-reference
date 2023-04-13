package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.language.higherKinds
import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.ast.JString
import com.socrata.prettyprint.prelude._

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.analyzer2

import DocUtils._

sealed abstract class Statement[MT <: MetaTypes] extends LabelUniverse[MT] {
  type Self[MT <: MetaTypes] <: Statement[MT]
  def asSelf: Self[MT]

  val schema: OrderedMap[AutoColumnLabel, NameEntry[CT]]

  // See the comment in From for an explanation of this.  It's just
  // labels here because, as a Statement, we don't have enough
  // information to produce a full Column.
  def unique: LazyList[Seq[AutoColumnLabel]]

  final def allTables: Set[DatabaseTableName] = doAllTables(Set.empty)
  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  final def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => types.DatabaseTableName[MT2],
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
  )(implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): Self[MT2] =
    doRewriteDatabaseNames(new RewriteDatabaseNamesState[MT2](tableName, columnName, realTables, changesOnlyLabels))

  /** The names that the SoQLAnalyzer produces aren't necessarily safe
    * for use in any particular database.  This lets those
    * automatically-generated names be systematically replaced. */
  final def relabel(using: LabelProvider): Self[MT] =
    doRelabel(new RelabelState(using))

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT]

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]]

  def isIsomorphic(that: Statement[MT]): Boolean =
    findIsomorphism(new IsomorphismState(this.realTables, that.realTables), None, None, that)

  private[analyzer2] def findIsomorphism(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT]
  ): Boolean

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]): Self[MT2]

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]]
  def contains(e: Expr[MT]): Boolean

  final def debugStr(implicit ev1: HasDoc[CV], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev1: HasDoc[CV], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]): StringBuilder =
    debugDoc.layoutSmart().toStringBuilder(sb)
  final def debugDoc(implicit ev1: HasDoc[CV], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]): Doc[Annotation[MT]] =
    doDebugDoc(new StatementDocProvider(ev1, ev2, ev3))

  private[analyzer2] def doDebugDoc(implicit ev: StatementDocProvider[MT]): Doc[Annotation[MT]]

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT]

  final def labelMap: LabelMap[MT] = {
    val state = new LabelMapState[MT]
    doLabelMap(state)
    state.build()
  }

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit
}

object Statement {
  implicit def serialize[MT <: MetaTypes](implicit writableRNS: Writable[MT#ResourceNameScope], writableCT: Writable[MT#ColumnType], writeableExpr: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[Statement[MT]] = new Writable[Statement[MT]] {
    def writeTo(buffer: WriteBuffer, stmt: Statement[MT]): Unit = {
      stmt match {
        case s: Select[MT] =>
          buffer.write(0)
          buffer.write(s)
        case v: Values[MT] =>
          buffer.write(1)
          buffer.write(v)
        case ct: CombinedTables[MT] =>
          buffer.write(2)
          buffer.write(ct)
        case cte: CTE[MT] =>
          buffer.write(3)
          buffer.write(cte)
      }
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableRNS: Readable[MT#ResourceNameScope], readableCT: Readable[MT#ColumnType], readableExpr: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[Statement[MT]] = new Readable[Statement[MT]] {
    def readFrom(buffer: ReadBuffer): Statement[MT] = {
      buffer.read[Int]() match {
        case 0 => buffer.read[Select[MT]]()
        case 1 => buffer.read[Values[MT]]()
        case 2 => buffer.read[CombinedTables[MT]]()
        case 3 => buffer.read[CTE[MT]]()
        case other => fail("Unknown statement tag " + other)
      }
    }
  }
}

case class CombinedTables[MT <: MetaTypes](
  op: TableFunc,
  left: Statement[MT],
  right: Statement[MT]
) extends Statement[MT] with statement.CombinedTablesImpl[MT] {
  require(left.schema.values.map(_.typ) == right.schema.values.map(_.typ))
}
object CombinedTables extends statement.OCombinedTablesImpl

case class CTE[MT <: MetaTypes](
  definitionLabel: AutoTableLabel,
  definitionAlias: Option[ResourceName], // can this ever be not-some?  If not, perhaps mapAlias's type needs changing
  definitionQuery: Statement[MT],
  materializedHint: MaterializedHint,
  useQuery: Statement[MT]
) extends Statement[MT] with statement.CTEImpl[MT]
object CTE extends statement.OCTEImpl

case class Values[MT <: MetaTypes](
  labels: OrderedSet[AutoColumnLabel],
  values: NonEmptySeq[NonEmptySeq[Expr[MT]]]
) extends Statement[MT] with statement.ValuesImpl[MT] {
  require(labels.size == values.head.length)
  require(values.tail.forall(_.length == values.head.length))
  require(values.tail.forall(_.iterator.zip(values.head.iterator).forall { case (a, b) => a.typ == b.typ }))
}
object Values extends statement.OValuesImpl

case class Select[MT <: MetaTypes](
  distinctiveness: Distinctiveness[MT],
  selectList: OrderedMap[AutoColumnLabel, NamedExpr[MT]],
  from: From[MT],
  where: Option[Expr[MT]],
  groupBy: Seq[Expr[MT]],
  having: Option[Expr[MT]],
  orderBy: Seq[OrderBy[MT]],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String],
  hint: Set[SelectHint]
) extends Statement[MT] with statement.SelectImpl[MT]

object Select extends statement.OSelectImpl
