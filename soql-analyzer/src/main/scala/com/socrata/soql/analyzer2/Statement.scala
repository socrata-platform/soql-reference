package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.language.higherKinds
import scala.util.parsing.input.{Position, NoPosition}

import com.rojoma.json.v3.ast.JString
import com.socrata.prettyprint.prelude._

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc
import com.socrata.soql.analyzer2.serialization.{Writable, WriteBuffer}

import DocUtils._

sealed abstract class Statement[+RNS, +CT, +CV] {
  type Self[+RNS, +CT, +CV] <: Statement[RNS, CT, CV]
  def asSelf: Self[RNS, CT, CV]

  val schema: OrderedMap[_ <: ColumnLabel, NameEntry[CT]]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  final def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Self[RNS, CT, CV] =
    doRewriteDatabaseNames(new RewriteDatabaseNamesState(realTables, tableName, columnName))

  /** The names that the SoQLAnalyzer produces aren't necessarily safe
    * for use in any particular database.  This lets those
    * automatically-generated names be systematically replaced. */
  final def relabel(using: LabelProvider): Self[RNS, CT, CV] =
    doRelabel(new RelabelState(using))

  private[analyzer2] def doRelabel(state: RelabelState): Self[RNS, CT, CV]

  /** For SQL forms that can refer to the select-columns by number, replace relevant
    * entries in those forms with the relevant select-column-index.
    *
    * e.g., this will rewrite a Statement that corresponds to "select
    * x+1, count(*) group by x+1 order by count(*)" to one that
    * corresponds to "select x+1, count(*) group by 1 order by 2"
    */
  def useSelectListReferences: Self[RNS, CT, CV]
  /** Undoes `useSelectListReferences`.  Note position information may
    * not roundtrip perfectly through these two calls. */
  def unuseSelectListReferences: Self[RNS, CT, CV]

  /** Remove columns that are not useful from inner selects.
    * SelectListReferences must not be present. */
  def removeUnusedColumns: Self[RNS, CT, CV] = doRemoveUnusedColumns(columnReferences, None)

  // If "myLabel" is "None" it means "keep all output columns"
  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]], myLabel: Option[TableLabel]): Self[RNS, CT, CV]

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]]

  def isIsomorphic[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](that: Statement[RNS2, CT2, CV2]): Boolean =
    findIsomorphism(new IsomorphismState, None, None, that)

  private[analyzer2] def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](
    state: IsomorphismState,
    thisCurrentTableLabel: Option[TableLabel],
    thatCurrentTableLabel: Option[TableLabel],
    that: Statement[RNS2, CT2, CV2]
  ): Boolean

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Self[RNS, CT, CV]

  private[analyzer2] def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[AutoColumnLabel], Self[RNS, CT2, CV])

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]]
  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder =
    debugDoc.layoutSmart().toStringBuilder(sb)
  def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[RNS, CT]]

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV]
}

object Statement {
  implicit def serialize[RNS: Writable, CT: Writable, CV: Writable]: Writable[Statement[RNS, CT, CV]] = new Writable[Statement[RNS, CT, CV]] {
    def writeTo(buffer: WriteBuffer, stmt: Statement[RNS, CT, CV]): Unit = {
      stmt match {
        case s: Select[RNS, CT, CV] =>
          buffer.write(0)
          Select.serialize[RNS, CT, CV].writeTo(buffer, s)
      }
    }
  }
}

case class CombinedTables[+RNS, +CT, +CV](
  op: TableFunc,
  left: Statement[RNS, CT, CV],
  right: Statement[RNS, CT, CV]
) extends Statement[RNS, CT, CV] with statement.CombinedTablesImpl[RNS, CT, CV] {
  require(left.schema.values.map(_.typ) == right.schema.values.map(_.typ))
}

case class CTE[+RNS, +CT, +CV](
  definitionLabel: AutoTableLabel,
  definitionQuery: Statement[RNS, CT, CV],
  materializedHint: MaterializedHint,
  useQuery: Statement[RNS, CT, CV]
) extends Statement[RNS, CT, CV] with statement.CTEImpl[RNS, CT, CV]

case class Values[+CT, +CV](
  values: NonEmptySeq[NonEmptySeq[Expr[CT, CV]]]
) extends Statement[Nothing, CT, CV] with statement.ValuesImpl[CT, CV] {
  require(values.tail.forall(_.length == values.head.length))
  require(values.tail.forall(_.iterator.zip(values.head.iterator).forall { case (a, b) => a.typ == b.typ }))
}

case class Select[+RNS, +CT, +CV](
  distinctiveness: Distinctiveness[CT, CV],
  selectList: OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]],
  from: From[RNS, CT, CV],
  where: Option[Expr[CT, CV]],
  groupBy: Seq[Expr[CT, CV]],
  having: Option[Expr[CT, CV]],
  orderBy: Seq[OrderBy[CT, CV]],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String],
  hint: Set[SelectHint]
) extends Statement[RNS, CT, CV] with statement.SelectImpl[RNS, CT, CV]

object Select extends statement.OSelectImpl
