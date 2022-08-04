package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.Position

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.NonEmptySeq

trait HasType[-CV, +CT] {
  def typeOf(cv: CV): CT
}

sealed abstract class Statement[+CT, +CV] {
  val schema: OrderedMap[ColumnLabel, NameEntry[CT]]

  private[analyzer2] def realTables: Map[TableLabel, DatabaseTableName]

  final def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Statement[CT, CV] =
    doRewriteDatabaseNames(realTables, tableName, columnName)

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
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

  private[analyzer2] def realTables: Map[TableLabel, DatabaseTableName] =
    left.realTables ++ right.realTables

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(
      left = left.doRewriteDatabaseNames(realTables, tableName, columnName),
      right = right.doRewriteDatabaseNames(realTables, tableName, columnName)
    )
}

sealed abstract class MaterializedHint
case object Materialized extends MaterializedHint
case object NotMaterialized extends MaterializedHint

case class CTE[+CT, +CV](
  definitionQuery: Statement[CT, CV],
  materializedHint: Option[MaterializedHint],
  useQuery: Statement[CT, CV]
) extends Statement[CT, CV] {
  val schema = useQuery.schema

  private[analyzer2] def realTables =
    definitionQuery.realTables ++ useQuery.realTables

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(
      definitionQuery = definitionQuery.doRewriteDatabaseNames(realTables, tableName, columnName),
      useQuery = useQuery.doRewriteDatabaseNames(realTables, tableName, columnName)
    )
}

case class Values[+CT, +CV](
  label: TableLabel,
  values: Seq[Expr[CT, CV]]
) extends Statement[CT, CV] {
  val schema = OrderedMap() ++ values.iterator.zipWithIndex.map { case (expr, idx) =>
    // I'm not sure if this is a postgresqlism or not, but in any event here we go...
    val name = s"column${idx+1}"
    DatabaseColumnName(name) -> NameEntry(ColumnName(name), expr.typ)
  }

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(
      values = values.map(_.doRewriteDatabaseNames(realTables, columnName))
    )
}

case class NamedExpr[+CT, +CV](expr: Expr[CT, CV], name: ColumnName) {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(expr = expr.doRewriteDatabaseNames(realTables, f))
}

case class Select[+CT, +CV](
  distinctiveness: Distinctiveness[CT, CV],
  selectList: OrderedMap[ColumnLabel, NamedExpr[CT, CV]],
  from: From[CT, CV],
  where: Option[Expr[CT, CV]],
  groupBy: Seq[Expr[CT, CV]],
  having: Option[Expr[CT, CV]],
  orderBy: Seq[OrderBy[CT, CV]],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String],
  hint: Set[SelectHint]
) extends Statement[CT, CV] {
  val schema = selectList.withValuesMapped { case NamedExpr(expr, name) => NameEntry(name, expr.typ) }

  private[analyzer2] def realTables = from.realTables

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) = {
    Select(
      distinctiveness = distinctiveness.doRewriteDatabaseNames(realTables, columnName),
      selectList = selectList.withValuesMapped(_.doRewriteDatabaseNames(realTables, columnName)),
      from = from.doRewriteDatabaseNames(realTables, tableName, columnName),
      where = where.map(_.doRewriteDatabaseNames(realTables, columnName)),
      groupBy = groupBy.map(_.doRewriteDatabaseNames(realTables, columnName)),
      having = having.map(_.doRewriteDatabaseNames(realTables, columnName)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(realTables, columnName)),
      limit = limit,
      offset = offset,
      search = search,
      hint = hint
    )
  }
}

sealed trait SelectHint
object SelectHint {
  case object Materialized extends SelectHint
  case object NoRollup extends SelectHint
  case object NoChainMerge extends SelectHint
}

sealed trait Distinctiveness[+CT, +CV] {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName): Distinctiveness[CT, CV]
}
object Distinctiveness {
  case object Indistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) = this
  }
  case object FullyDistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) = this
  }
  case class On[+CT, +CV](exprs: Seq[Expr[CT, CV]]) extends Distinctiveness[CT, CV] {
    private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
      On(exprs.map(_.doRewriteDatabaseNames(realTables, f)))
  }
}

case class OrderBy[+CT, +CV](expr: Expr[CT, CV], ascending: Boolean, nullLast: Boolean) {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(expr = expr.doRewriteDatabaseNames(realTables, f))
}

sealed abstract class From[+CT, +CV] {
  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Either[AddScopeError, Environment[CT2]]

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): From[CT, CV]

  private[analyzer2] def realTables: Map[TableLabel, DatabaseTableName]
}
case class Join[+CT, +CV](joinType: JoinType, lateral: Boolean, left: AtomicFrom[CT, CV], right: From[CT, CV], on: Expr[CT, CV]) extends From[CT, CV] {
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
    def loop(acc: Environment[CT2], self: From[CT2, CV]): Either[AddScopeError, Environment[CT2]] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          acc.addScope(left.alias, left.scope) match {
            case Right(env) => loop(env, right)
            case Left(err) => Left(err)
          }
        case other: AtomicFrom[CT2, CV] =>
          other.addToEnvironment(acc)
      }
    }
    loop(base.extend, this)
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

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): From[CT, CV] = {
    type Stack = List[From[CT, CV] => Join[CT, CV]]

    @tailrec
    def loop(self: From[CT, CV], stack: Stack): From[CT, CV] =
      self match {
        case Join(joinType, lateral, left, right, on) =>
          val newLeft = left.doRewriteDatabaseNames(realTables, tableName, columnName)
          val newOn = on.doRewriteDatabaseNames(realTables, columnName)
          loop(right, { newRight: From[CT, CV] => Join(joinType, lateral, newLeft, newRight, newOn) } :: stack)
        case nonJoin =>
          stack.foldLeft(nonJoin.doRewriteDatabaseNames(realTables, tableName, columnName)) { (acc, f) =>
            f(acc)
          }
      }

    loop(this, Nil)
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
  val alias: Option[ResourceName]
  val label: TableLabel

  private[analyzer2] val scope: Scope[CT]

  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = {
    addToEnvironment(base.extend)
  }
  private[analyzer2] def addToEnvironment[CT2 >: CT](env: Environment[CT2]) = {
    env.addScope(alias, scope)
  }

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): AtomicFrom[CT, CV]

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): AtomicFrom[CT, CV]
}
case class FromTable[+CT](tableName: DatabaseTableName, alias: Option[ResourceName], label: TableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends AtomicFrom[CT, Nothing] {
  private[analyzer2] val scope: Scope[CT] = Scope(columns, label)

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
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
  private[analyzer2] val scope: Scope[CT] = Scope(statement.schema, label)

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    copy(statement = statement.doRewriteDatabaseNames(realTables, tableName, columnName))

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromStatement[CT, CV] =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map.empty
}
case class FromSingleRow(label: TableLabel, alias: Option[ResourceName]) extends AtomicFrom[Nothing, Nothing] {
  private[analyzer2] val scope: Scope[Nothing] =
    Scope(
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  private[analyzer2] def doRewriteDatabaseNames(
    realTables: Map[TableLabel, DatabaseTableName],
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ) =
    this

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromSingleRow =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map.empty
}

sealed abstract class Expr[+CT, +CV] {
  val typ: CT
  val position: Position

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName): Expr[CT, CV]
}
case class Column[+CT](table: TableLabel, column: ColumnLabel, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    column match {
      case dcn: DatabaseColumnName =>
        copy(column = f(realTables(table), dcn))(position)
      case _ =>
        this
    }
}

sealed abstract class Literal[+CT, +CV] extends Expr[CT, CV] {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this
}
case class LiteralValue[+CT, +CV](value: CV)(val position: Position)(implicit ev: HasType[CV, CT]) extends Literal[CT, CV] {
  val typ = ev.typeOf(value)
}
case class NullLiteral[+CT](typ: CT)(val position: Position) extends Literal[CT, Nothing]

sealed trait FuncallLike[+CT, +CV] extends Expr[CT, CV] {
  val function: MonomorphicFunction[CT]
  val functionNamePosition: Position
}

case class FunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  require(!function.isAggregate)
  val typ = function.result

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(args = args.map(_.doRewriteDatabaseNames(realTables, f)))(position, functionNamePosition)
}
case class AggregateFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  distinct: Boolean,
  filter: Option[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  require(function.isAggregate)
  val typ = function.result

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(realTables, f)),
      filter = filter.map(_.doRewriteDatabaseNames(realTables, f))
    )(position, functionNamePosition)
}
case class WindowedFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  partitionBy: Seq[Expr[CT, CV]], // is normal right here, or should it be aggregate?
  orderBy: Seq[OrderBy[CT, CV]], // ditto thus
  context: FrameContext,
  start: FrameBound,
  end: Option[FrameBound],
  exclusion: Option[FrameExclusion]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  val typ = function.result

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(realTables, f)),
      partitionBy = partitionBy.map(_.doRewriteDatabaseNames(realTables, f)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(realTables, f))
    )(position, functionNamePosition)
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
