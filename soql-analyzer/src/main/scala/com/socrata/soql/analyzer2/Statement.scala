package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.NonEmptySeq

trait HasType[-CV, +CT] {
  def typeOf(cv: CV): CT
}

sealed abstract class Statement[+CT, +CV] {
  val schema: OrderedMap[ColumnLabel, NameEntry[CT]]
  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Statement[CT, CV]
}

sealed abstract class TableFunc
case object Union extends TableFunc
case object UnionAll extends TableFunc
case object Intersect extends TableFunc
case object IntersectAll extends TableFunc
case object Difference extends TableFunc
case object DifferenceAll extends TableFunc

case class CombinedTables[+CT, +CV](op: TableFunc, left: Statement[CT, CV], right: Statement[CT, CV]) extends Statement[CT, CV] {
  require(left.schema.values.map(_.typ) == right.schema.values.map(_.typ))
  val schema = left.schema

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(
      left = left.rewriteDatabaseNames(tableName, columnName),
      right = right.rewriteDatabaseNames(tableName, columnName)
    )
}

sealed abstract class MaterializedHint
case object Materialized extends MaterializedHint
case object NotMaterialized extends MaterializedHint

case class CTE[+CT, +CV](
  label: TableLabel,
  definitionQuery: Statement[CT, CV],
  materializedHint: Option[MaterializedHint],
  useQuery: Statement[CT, CV]
) extends Statement[CT, CV] {
  val schema = useQuery.schema

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(
      definitionQuery = definitionQuery.rewriteDatabaseNames(tableName, columnName),
      useQuery = useQuery.rewriteDatabaseNames(tableName, columnName)
    )
}

case class NamedExpr[+CT, +CV, +Ctx <: Windowed](expr: Expr[CT, CV, Ctx], name: ColumnName) {
  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(expr = expr.rewriteDatabaseNames(schemas, f))
}

case class Select[+CT, +CV](
  selectList: OrderedMap[ColumnLabel, NamedExpr[CT, CV, Windowed]],
  from: From[CT, CV],
  where: Option[Expr[CT, CV, Normal]],
  groupBy: Seq[Expr[CT, CV, Normal]],
  having: Option[Expr[CT, CV, Aggregate]],
  orderBy: Seq[OrderBy[CT, CV, Windowed]],
  limit: Option[BigInt],
  offset: Option[BigInt]
) extends Statement[CT, CV] {
  val schema = selectList.withValuesMapped { case NamedExpr(expr, name) => NameEntry(name, expr.typ) }

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) = {
    val namespace = from.realTables
    Select(
      selectList = selectList.withValuesMapped(_.rewriteDatabaseNames(namespace, columnName)),
      from = from.rewriteDatabaseNames(tableName, columnName),
      where = where.map(_.rewriteDatabaseNames(namespace, columnName)),
      groupBy = groupBy.map(_.rewriteDatabaseNames(namespace, columnName)),
      having = having.map(_.rewriteDatabaseNames(namespace, columnName)),
      orderBy = orderBy.map(_.rewriteDatabaseNames(namespace, columnName)),
      limit = limit,
      offset = offset
    )
  }
}

case class OrderBy[+CT, +CV, +Ctx <: Windowed](expr: Expr[CT, CV, Ctx], ascending: Boolean, nullLast: Boolean) {
  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(expr = expr.rewriteDatabaseNames(schemas, f))
}

sealed abstract class From[+CT, +CV] {
  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Environment[CT2]

  private[analyzer2] def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): From[CT, CV]

  private[analyzer2] def realTables: Map[TableLabel, DatabaseTableName]
}
case class Join[+CT, +CV](joinType: JoinType, lateral: Boolean, left: AtomicFrom[CT, CV], right: From[CT, CV], on: Expr[CT, CV, Normal]) extends From[CT, CV] {
  // The difference between a lateral and a non-lateral join is the
  // environment assumed while typechecking; in a non-lateral join
  // it's something like:
  //    val checkedLeft = left.typeCheckIn(enclosingEnv)
  //    val checkedRight = right.typeCheckIn(enclosingEnv)
  // whereas in a lateral join it's like
  //    val checkedLeft = left.typecheckIn(enclosingEnv)
  //    val checkedRight = right.typecheckIn(checkedLeft.extendEnvironment(previousFromEnv))
  // In both cases the "next" FROM env (where "on" is typechecked) is
  //    val nextFromEnv = checkedRight.extendEnvironment(checkedLeft.extendEnvironment(previousFromEnv))
  // which is what this `extendEnvironment` function does, rewritten
  // as a loop so that a lot of joins don't use a lot of stack.
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = {
    @tailrec
    def loop(acc: Environment[CT2], self: From[CT2, CV]): Environment[CT2] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          loop(left.extendEnvironment(acc), right)
        case other =>
          other.extendEnvironment(acc)
      }
    }
    loop(base, this)
  }

  private[analyzer2] def realTables: Map[TableLabel, DatabaseTableName] = {
    @tailrec
    def loop(acc: Map[TableLabel, DatabaseTableName], self: From[CT, CV]): Map[TableLabel, DatabaseTableName] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          loop(acc ++ left.realTables, right)
        case other =>
          acc ++ other.realTables
      }
    }
    loop(Map.empty, this)
  }

  private[analyzer2] def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): From[CT, CV] = {
    // annoying...
    copy(left = left.rewriteDatabaseNames(tableName, columnName), right = right.rewriteDatabaseNames(tableName, columnName))
  }
}

sealed abstract class JoinType
object JoinType {
  case object Inner extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
}

case class DatabaseTableName(name: String) {
  def asResourceName = ResourceName(name)
}

sealed abstract class AtomicFrom[+CT, +CV] extends From[CT, CV] {
  protected val scope: Scope[CT]
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = base.extend(scope)

  val label: TableLabel

  private[analyzer2] def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): AtomicFrom[CT, CV]

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): AtomicFrom[CT, CV]
}
case class FromTable[+CT](tableName: DatabaseTableName, alias: Option[ResourceName], label: TableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends AtomicFrom[CT, Nothing] {
  protected val scope: Scope[CT] =
    Scope(Some(alias.getOrElse(tableName.asResourceName)), columns, label)

  private[analyzer2] def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(
      tableName = tableName(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => columnName(this.tableName, n) -> ne }
    )

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromTable[CT] =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map(label -> tableName)
}
// "alias" is optional here because of chained soql; actually having a
// real subselect syntactically requires an alias, but `select ... |>
// select ...` does not.  The alias is just for name-resolution during
// analysis anyway...
case class FromStatement[+CT, +CV](statement: Statement[CT, CV], label: TableLabel, alias: Option[ResourceName]) extends AtomicFrom[CT, CV] {
  protected val scope: Scope[CT] =
    Scope(alias, statement.schema, label)

  private[analyzer2] def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(statement = statement.rewriteDatabaseNames(tableName, columnName))

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromStatement[CT, CV] =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map.empty
}
case class FromSingleRow(label: TableLabel, alias: Option[ResourceName]) extends AtomicFrom[Nothing, Nothing] {
  protected val scope: Scope[Nothing] =
    Scope(
      Some(alias.getOrElse(ResourceName(TableName(TableName.SingleRow).nameWithoutPrefix))),
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  private[analyzer2] def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    this

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromSingleRow =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map.empty
}

// Expressions
//
// This is a little icky because I didn't make it as flexible as I
// would like, but as far as I can tell this subclassing relationship
// between window/aggregate/other expressions is in fact accurate

sealed abstract class Windowed
sealed abstract class Aggregate extends Windowed
sealed abstract class Normal extends Aggregate

sealed abstract class Expr[+CT, +CV, +Ctx <: Windowed] {
  val typ: CT

  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName): Expr[CT, CV, Ctx]
}
case class Column[+CT](table: TableLabel, column: ColumnLabel, typ: CT) extends Expr[CT, Nothing, Normal] {
  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    column match {
      case dcn: DatabaseColumnName =>
        copy(column = f(schemas(table), dcn))
      case _ =>
        this
    }
}

sealed abstract class Literal[+CT, +CV] extends Expr[CT, CV, Normal] {
  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this
}
case class LiteralValue[+CT, +CV](value: CV)(implicit ev: HasType[CV, CT]) extends Literal[CT, CV] {
  val typ = ev.typeOf(value)
}
case class NullLiteral[+CT](typ: CT) extends Literal[CT, Nothing]

case class FunctionCall[+CT, +CV, +Ctx <: Windowed](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV, Ctx]] // a normal function call does not change the current context, so things like `1 + sum(xs)` is legal
) extends Expr[CT, CV, Ctx] {
  require(!function.isAggregate)
  val typ = function.result

  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(args = args.map(_.rewriteDatabaseNames(schemas, f)))
}
case class AggregateFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV, Normal]],
  distinct: Boolean,
  filter: Option[Expr[CT, CV, Normal]]
) extends Expr[CT, CV, Aggregate] {
  require(function.isAggregate)
  val typ = function.result

  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(args = args.map(_.rewriteDatabaseNames(schemas, f)))
}
case class WindowedFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV, Aggregate]],
  partitionBy: Seq[Expr[CT, CV, Normal]], // is normal right here, or should it be aggregate?
  orderBy: Seq[OrderBy[CT, CV, Normal]], // ditto thus
  context: FrameContext,
  start: FrameBound,
  end: Option[FrameBound],
  exclusion: Option[FrameExclusion]
) extends Expr[CT, CV, Windowed] {
  val typ = function.result

  private[analyzer2] def rewriteDatabaseNames(schemas: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(args = args.map(_.rewriteDatabaseNames(schemas, f)))
}

sealed abstract class FrameContext
object FrameContext {
  case object Range extends FrameContext
  case object Rows extends FrameContext
  case object Groups extends FrameContext
}

sealed abstract class FrameBound
object FrameBound {
  case object UnboundedPreceding extends FrameBound
  case class Preceding(n: Long) extends FrameBound
  case object CurrentRow extends FrameBound
  case class Following(n: Long) extends FrameBound
  case object UnboundedFollowing extends FrameBound
}

sealed abstract class FrameExclusion
object FrameExclusion {
  case object CurrentRow extends FrameExclusion
  case object Group extends FrameExclusion
  case object Ties extends FrameExclusion
  case object NoOthers extends FrameExclusion
}
