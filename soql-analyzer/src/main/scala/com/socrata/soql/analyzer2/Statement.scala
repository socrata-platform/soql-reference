package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.Position

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}

sealed abstract class Statement[+CT, +CV] {
  type Self[+CT, +CV] <: Statement[CT, CV]

  val schema: OrderedMap[_ <: ColumnLabel, NameEntry[CT]]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  final def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Self[CT, CV] =
    doRewriteDatabaseNames(new RewriteDatabaseNamesState(realTables, tableName, columnName))

  /** The names that the SoQLAnalyzer produces aren't necessarily safe
    * for use in any particular database.  This lets those
    * automatically-generated names be systematically replaced. */
  final def relabel(using: LabelProvider): Self[CT, CV] =
    doRelabel(new RelabelState(using))

  private[analyzer2] def doRelabel(state: RelabelState): Self[CT, CV]

  /** For SQL forms that can refer to the select-columns by number, replace relevant
    * entries in those forms with the relevant select-column-index.
    *
    * e.g., this will rewrite a Statement that corresponds to "select
    * x+1, count(*) group by x+1 order by count(*)" to one that
    * corresponds to "select x+1, count(*) group by 1 order by 2"
    */
  def numericate: Self[CT, CV]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Self[CT, CV]

  final def debugStr: String = debugStr(new StringBuilder).toString
  def debugStr(sb: StringBuilder): StringBuilder
}

case class CombinedTables[+CT, +CV](op: TableFunc, left: Statement[CT, CV], right: Statement[CT, CV]) extends Statement[CT, CV] {
  require(left.schema.values.map(_.typ) == right.schema.values.map(_.typ))

  type Self[+CT, +CV] = CombinedTables[CT, CV]

  val schema = left.schema

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] =
    left.realTables ++ right.realTables

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      left = left.doRewriteDatabaseNames(state),
      right = right.doRewriteDatabaseNames(state)
    )

  private[analyzer2] def doRelabel(state: RelabelState): CombinedTables[CT, CV] =
    copy(left = left.doRelabel(state), right = right.doRelabel(state))

  def numericate = copy(left = left.numericate, right = right.numericate)

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

case class CTE[+CT, +CV](
  definitionLabel: AutoTableLabel,
  definitionQuery: Statement[CT, CV],
  materializedHint: MaterializedHint,
  useQuery: Statement[CT, CV]
) extends Statement[CT, CV] {
  type Self[+CT, +CV] = CTE[CT, CV]

  val schema = useQuery.schema

  private[analyzer2] def realTables =
    definitionQuery.realTables ++ useQuery.realTables

  def numericate = copy(definitionQuery = definitionQuery.numericate, useQuery = useQuery.numericate)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      definitionQuery = definitionQuery.doRewriteDatabaseNames(state),
      useQuery = useQuery.doRewriteDatabaseNames(state)
    )

  private[analyzer2] def doRelabel(state: RelabelState): CTE[CT, CV] =
    copy(definitionLabel = state.convert(definitionLabel),
         definitionQuery = definitionQuery.doRelabel(state),
         useQuery = useQuery.doRelabel(state))

  override def debugStr(sb: StringBuilder) = {
    sb.append( "WITH ").append(definitionLabel).append(" AS ")
    materializedHint match {
      case MaterializedHint.Default => // ok
      case MaterializedHint.Materialized => sb.append("MATERIALIZED ")
      case MaterializedHint.NotMaterialized => sb.append("NOT MATERIALIZED ")
    }
    sb.append("(")
    definitionQuery.debugStr(sb)
    sb.append(") ")
    useQuery.debugStr(sb)
  }
}

case class Values[+CT, +CV](
  values: NonEmptySeq[NonEmptySeq[Expr[CT, CV]]]
) extends Statement[CT, CV] {
  require(values.tail.forall(_.length == values.head.length))
  require(values.tail.forall(_.iterator.zip(values.head.iterator).forall { case (a, b) => a.typ == b.typ }))

  type Self[+CT, +CV] = Values[CT, CV]

  // This lets us see the schema with DatabaseColumnNames as keys
  def typeVariedSchema[T >: DatabaseColumnName]: OrderedMap[T, NameEntry[CT]] =
    OrderedMap() ++ values.head.iterator.zipWithIndex.map { case (expr, idx) =>
      // This is definitely a postgresqlism, unfortunately
      val name = s"column${idx+1}"
      DatabaseColumnName(name) -> NameEntry(ColumnName(name), expr.typ)
    }

  val schema = typeVariedSchema

  def numericate = this

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      values = values.map(_.map(_.doRewriteDatabaseNames(state)))
    )

  private[analyzer2] def doRelabel(state: RelabelState): Values[CT, CV] =
    copy(values = values.map(_.map(_.doRelabel(state))))

  override def debugStr(sb: StringBuilder) = {
    sb.append("values ")
    var didOne = false
    for(list <- values) {
      if(didOne) sb.append(", ")
      else didOne = true

      sb.append('(')
      var didExpr = false
      for(expr <- list) {
        if(didExpr) sb.append(", ")
        else didExpr = true
        expr.debugStr(sb)
      }
      sb.append(')')
    }
    sb
  }
}

case class Select[+CT, +CV](
  distinctiveness: Distinctiveness[CT, CV],
  selectList: OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]],
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
  type Self[+CT, +CV] = Select[CT, CV]

  val schema = selectList.withValuesMapped { case NamedExpr(expr, name) => NameEntry(name, expr.typ) }

  def isAggregated =
    groupBy.nonEmpty ||
      having.nonEmpty ||
      selectList.valuesIterator.exists(_.expr.isAggregated) ||
      orderBy.iterator.exists(_.expr.isAggregated)

  private[analyzer2] def realTables = from.realTables

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = {
    Select(
      distinctiveness = distinctiveness.doRewriteDatabaseNames(state),
      selectList = selectList.withValuesMapped(_.doRewriteDatabaseNames(state)),
      from = from.doRewriteDatabaseNames(state),
      where = where.map(_.doRewriteDatabaseNames(state)),
      groupBy = groupBy.map(_.doRewriteDatabaseNames(state)),
      having = having.map(_.doRewriteDatabaseNames(state)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(state)),
      limit = limit,
      offset = offset,
      search = search,
      hint = hint
    )
  }

  private[analyzer2] def doRelabel(state: RelabelState) =
    Select(
      distinctiveness = distinctiveness.doRelabel(state),
      selectList = OrderedMap() ++ selectList.iterator.map { case (k, v) => state.convert(k) -> v.doRelabel(state) },
      from = from.doRelabel(state),
      where = where.map(_.doRelabel(state)),
      groupBy = groupBy.map(_.doRelabel(state)),
      having = having.map(_.doRelabel(state)),
      orderBy = orderBy.map(_.doRelabel(state)),
      limit = limit,
      offset = offset,
      search = search,
      hint = hint
    )

  def numericate: Select[CT, CV] = {
    def numericateExpr(e: Expr[CT, CV]): Expr[CT, CV] = {
      selectList.valuesIterator.map(_.expr).zipWithIndex.find { case (e2, _idx) => e2 == e } match {
        case Some((expr, idx)) => SelectListReference(idx+1, expr.isAggregated, expr.typ)(expr.position)
        case None => e
      }
    }

    copy(
      distinctiveness = distinctiveness match {
        case Distinctiveness.Indistinct | Distinctiveness.FullyDistinct => distinctiveness
        case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(numericateExpr))
      },
      from = from.numericate,
      groupBy = groupBy.map(numericateExpr),
      orderBy = orderBy.map { ob => ob.copy(expr = numericateExpr(ob.expr)) }
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
