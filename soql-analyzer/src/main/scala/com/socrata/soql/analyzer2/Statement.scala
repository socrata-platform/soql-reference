package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.Position

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasType
import com.socrata.NonEmptySeq

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

  final def debugStr: String = debugStr(new StringBuilder).toString
  def debugStr(sb: StringBuilder): StringBuilder
}

sealed abstract class TableFunc
object TableFunc {
  case object Union extends TableFunc
  case object UnionAll extends TableFunc
  case object Intersect extends TableFunc
  case object IntersectAll extends TableFunc
  case object Minus extends TableFunc
  case object MinusAll extends TableFunc
}

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

  override def debugStr(sb: StringBuilder) = {
    sb.append('(')
    left.debugStr(sb)
    sb.append(") ")
    sb.append(op.toString)
    sb.append(" (")
    right.debugStr(sb)
    sb.append(")")
  }
}

sealed abstract class MaterializedHint
case object Materialized extends MaterializedHint
case object NotMaterialized extends MaterializedHint

case class CTE[+CT, +CV](
  definitionLabel: TableLabel,
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

  override def debugStr(sb: StringBuilder) = {
    sb.append( "WITH ").append(definitionLabel).append(" AS (")
    definitionQuery.debugStr(sb)
    sb.append(") ")
    useQuery.debugStr(sb)
  }
}

case class Values[+CT, +CV](
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

  override def debugStr(sb: StringBuilder) = {
    sb.append( "values (")
    var didOne = false
    for(expr <- values) {
      if(didOne) sb.append(", ")
      else didOne = true
      expr.debugStr(sb)
    }
    sb.append(")")
  }
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

  def debugStr(sb: StringBuilder) = {
    sb.append("SELECT ")

    distinctiveness.debugStr(sb)

    {
      var didOne = false
      for((col, expr) <- selectList) {
        if(didOne) sb.append(", ")
        else didOne = true
        expr.expr.debugStr(sb).append(" AS ").append(col)
      }
    }

    sb.append(" FROM ")
    from.debugStr(sb)

    for(w <- where) {
      sb.append(" WHERE ")
      w.debugStr(sb)
    }

    if(groupBy.nonEmpty) {
      sb.append(" GROUP BY ")
      var didOne = false
      for(gb <- groupBy) {
        if(didOne) sb.append(", ")
        else didOne = true
        gb.debugStr(sb)
      }
    }

    for(h <- having) {
      sb.append(" HAVING ")
      h.debugStr(sb)
    }

    if(orderBy.nonEmpty) {
      sb.append(" ORDER BY ")
      var didOne = false
      for(ob <- orderBy) {
        if(didOne) sb.append(", ")
        else didOne = true
        ob.debugStr(sb)
      }
    }

    for(l <- offset) {
      sb.append(" OFFSET ").append(l)
    }
    for(l <- limit) {
      sb.append(" LIMIT ").append(l)
    }
    for(s <- search) {
      sb.append(" SEARCH ").append(com.rojoma.json.v3.ast.JString(s))
    }

    sb
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
  def debugStr(sb: StringBuilder): StringBuilder
}
object Distinctiveness {
  case object Indistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) = this
    def debugStr(sb: StringBuilder): StringBuilder = sb
  }
  case object FullyDistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) = this
    def debugStr(sb: StringBuilder): StringBuilder = sb.append("DISTINCT ")
  }
  case class On[+CT, +CV](exprs: Seq[Expr[CT, CV]]) extends Distinctiveness[CT, CV] {
    private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
      On(exprs.map(_.doRewriteDatabaseNames(realTables, f)))
    def debugStr(sb: StringBuilder): StringBuilder = {
      sb.append("DISTINCT ON (")
      var didOne = false
      for(expr <- exprs) {
        if(didOne) sb.append(", ")
        else didOne = true
        expr.debugStr(sb)
      }
      sb.append(") ")
    }
  }
}

case class OrderBy[+CT, +CV](expr: Expr[CT, CV], ascending: Boolean, nullLast: Boolean) {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(expr = expr.doRewriteDatabaseNames(realTables, f))

  def debugStr(sb: StringBuilder): StringBuilder = {
    expr.debugStr(sb)
    if(ascending) {
      sb.append(" ASC")
    } else {
      sb.append(" DESC")
    }
    if(nullLast) {
      sb.append(" NULLS LAST")
    } else {
      sb.append(" NULLS FIRST")
    }
  }
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

  def debugStr(sb: StringBuilder): StringBuilder
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

  def debugStr(sb: StringBuilder): StringBuilder = {
    left.debugStr(sb)
    def loop(prevJoin: Join[CT, CV], from: From[CT, CV]): StringBuilder = {
      from match {
        case j: Join[CT, CV] =>
          sb.
            append(' ').
            append(prevJoin.joinType).
            append(if(prevJoin.lateral) " LATERAL" else "").
            append(' ')
          j.left.debugStr(sb).
            append(" ON ")
          prevJoin.on.debugStr(sb)
          loop(j, j.right)
        case nonJoin =>
          sb.append(' ').
            append(prevJoin.joinType).
            append(if(prevJoin.lateral) " LATERAL" else "").
            append(' ')
          nonJoin.debugStr(sb).
            append(" ON ")
          prevJoin.on.debugStr(sb)
      }
    }
    loop(this, this.right)
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

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(tableName).append(" AS ").append(label)
  }
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

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append('(')
    statement.debugStr(sb)
    sb.append(") AS ").append(label)
  }
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

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append("@single_row")
  }
}

sealed abstract class Expr[+CT, +CV] {
  val typ: CT
  val position: Position

  val size: Int

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName): Expr[CT, CV]

  def debugStr(sb: StringBuilder): StringBuilder
}
case class Column[+CT](table: TableLabel, column: ColumnLabel, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    column match {
      case dcn: DatabaseColumnName =>
        realTables.get(table) match {
          case Some(table) => copy(column = f(table, dcn))(position)
          case None => this // This can happen thanks to UDFs, which has DatabaseColumNames but no associated DatabaseTableName.
        }
      case _ =>
        this
    }

  val size = 1

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append(table).append('.').append(column)
}

sealed abstract class Literal[+CT, +CV] extends Expr[CT, CV] {
  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this
}
case class LiteralValue[+CT, +CV](value: CV)(val position: Position)(implicit ev: HasType[CV, CT]) extends Literal[CT, CV] {
  val typ = ev.typeOf(value)
  val size = 1

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append(value)
}
case class NullLiteral[+CT](typ: CT)(val position: Position) extends Literal[CT, Nothing] {
  val size = 1

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append("NULL")
}

sealed trait FuncallLike[+CT, +CV] extends Expr[CT, CV] with Product {
  val function: MonomorphicFunction[CT]
  val functionNamePosition: Position

  override final def equals(that: Any): Boolean =
    that match {
      case null => false
      case thing: AnyRef if thing eq this => true // short circuit identity
      case thing if thing.getClass == this.getClass =>
        this.productIterator.zip(thing.asInstanceOf[Product].productIterator).forall { case (a, b) => a == b }
      case _ => false
    }
}

case class FunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  require(!function.isAggregate)
  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(args = args.map(_.doRewriteDatabaseNames(realTables, f)))(position, functionNamePosition)

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(function.name).append('(')
    var didOne = false
    for(arg <- args) {
      if(didOne) sb.append(", ")
      else didOne = true
      arg.debugStr(sb)
    }
    sb.append(')')
  }
}
case class AggregateFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  distinct: Boolean,
  filter: Option[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  require(function.isAggregate)
  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum + filter.fold(0)(_.size)

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(realTables, f)),
      filter = filter.map(_.doRewriteDatabaseNames(realTables, f))
    )(position, functionNamePosition)

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(function.name).append('(')
    if(distinct) {
      sb.append("DISTINCT ")
    }
    var didOne = false
    for(arg <- args) {
      if(didOne) sb.append(", ")
      else didOne = true
      arg.debugStr(sb)
    }
    sb.append(')')
    for(f <- filter) {
      sb.append(" FILTER (WHERE ")
      f.debugStr(sb)
      sb.append(')')
    }
    sb
  }
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

  val size = 1 + args.iterator.map(_.size).sum + partitionBy.iterator.map(_.size).sum + orderBy.iterator.map(_.expr.size).sum

  private[analyzer2] def doRewriteDatabaseNames(realTables: Map[TableLabel, DatabaseTableName], f: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(realTables, f)),
      partitionBy = partitionBy.map(_.doRewriteDatabaseNames(realTables, f)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(realTables, f))
    )(position, functionNamePosition)

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append("[WINDOW FUNCTION STUFF]")
  }
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
