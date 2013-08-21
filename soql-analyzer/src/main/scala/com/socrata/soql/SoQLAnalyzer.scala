package com.socrata.soql

import scala.util.parsing.input.NoPosition

import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.aggregates.AggregateChecker
import com.socrata.soql.ast._
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typechecker._
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.collection.OrderedMap

class SoQLAnalyzer[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type]) {
  type Analysis = SoQLAnalysis[ColumnName, Type]
  type Expr = typed.CoreExpr[ColumnName, Type]

  val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLAnalyzer[_]])
  def ns2ms(ns: Long) = ns / 1000000
  val aggregateChecker = new AggregateChecker[Type]

  /** Turn a SoQL SELECT statement into a typed `Analysis` object.
    * @param query The SELECT to parse and analyze
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous
    * @return The analysis of the query */
  def analyzeFullQuery(query: String)(implicit ctx: DatasetContext[Type]): Analysis = {
    log.debug("Analyzing full query {}", query)
    val start = System.nanoTime()
    val parsed = new Parser().selectStatement(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))

    analyzeWithSelection(parsed)
  }

  /** Turn framents of a SoQL SELECT statement into a typed `Analysis` object.  If no `selection` is provided,
    * one is generated based on whether the rest of the parameters indicate an aggregate query or not.  If it
    * is not an aggregate query, a selection list equivalent to `*` (i.e., every non-system column) is generated.
    * If it is an aggregate query, a selection list made up of the expressions from `groupBy` (if provided) together
    * with "`count(*)`" is generated.
    *
    * @param selection A selection list.
    * @param where An expression to be used as the query's WHERE clause.
    * @param groupBy A comma-separated list of expressions to be used as the query's GROUP BY cluase.
    * @param having An expression to be used as the query's HAVING clause.
    * @param orderBy A comma-separated list of expressions and sort-order specifiers to be uesd as the query's ORDER BY clause.
    * @param limit A non-negative integer to be used as the query's LIMIT parameter
    * @param offset A non-negative integer to be used as the query's OFFSET parameter
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous.
    * @return The analysis of the query. */
  def analyzeSplitQuery(selection: Option[String],
                        where: Option[String],
                        groupBy: Option[String],
                        having: Option[String],
                        orderBy: Option[String],
                        limit: Option[String],
                        offset: Option[String],
                        search: Option[String])(implicit ctx: DatasetContext[Type]): Analysis =
  {
    log.debug("analyzing split query")

    val p = new Parser

    def dispatch(selection: Option[Selection], where: Option[Expression], groupBy: Option[Seq[Expression]], having: Option[Expression], orderBy: Option[Seq[OrderBy]], limit: Option[BigInt], offset: Option[BigInt], search: Option[String]) =
      selection match {
        case None => analyzeNoSelection(where, groupBy, having, orderBy, limit, offset, search)
        case Some(s) => analyzeWithSelection(Select(s, where, groupBy, having, orderBy, limit, offset, search))
      }

    dispatch(
      selection.map(p.selection),
      where.map(p.expression),
      groupBy.map(p.groupBys),
      having.map(p.expression),
      orderBy.map(p.orderings),
      limit.map(p.limit),
      offset.map(p.offset),
      search.map(p.search)
    )
  }

  def analyzeNoSelection(where: Option[Expression],
                         groupBy: Option[Seq[Expression]],
                         having: Option[Expression],
                         orderBy: Option[Seq[OrderBy]],
                         limit: Option[BigInt],
                         offset: Option[BigInt],
                         search: Option[String])(implicit ctx: DatasetContext[Type]): Analysis =
  {
    log.debug("No selection; doing typechecking of the other parts then deciding what to make the selection")

    // ok, so the only tricky thing here is the selection itself.  Since it isn't provided, there are two cases:
    //   1. If there are groupBys, having, or aggregates in orderBy, then selection should be the equivalent of
    //      selecting the group-by clauses together with "count(*)".
    //   2. Otherwise, it should be the equivalent of selecting "*".
    val typechecker = new Typechecker(typeInfo, functionInfo)
    val t0 = System.nanoTime()
    val checkedWhere = where.map(typechecker(_, Map.empty))
    val t1 = System.nanoTime()
    val checkedGroupBy = groupBy.map(_.map(typechecker(_, Map.empty)))
    val t2 = System.nanoTime()
    val checkedHaving = having.map(typechecker(_, Map.empty))
    val t3 = System.nanoTime()
    val checkedOrderBy = orderBy.map { obs => obs.zip(obs.map { ob => typechecker(ob.expression, Map.empty) }) }
    val t4 = System.nanoTime()
    val isGrouped = aggregateChecker(Nil, checkedWhere, checkedGroupBy, checkedHaving, checkedOrderBy.getOrElse(Nil).map(_._2))
    val t5 = System.nanoTime()

    if(log.isTraceEnabled) {
      log.trace("typechecking WHERE took {}ms", ns2ms(t1 - t0))
      log.trace("typechecking GROUP BY took {}ms", ns2ms(t2 - t1))
      log.trace("typechecking HAVING took {}ms", ns2ms(t3 - t2))
      log.trace("typechecking ORDER BY took {}ms", ns2ms(t4 - t3))
      log.trace("checking for aggregation took {}ms", ns2ms(t5 - t4))
    }

    val (names, typedSelectedExpressions) = if(isGrouped) {
      log.debug("It is grouped; selecting the group bys plus count(*)")

      val beforeAliasAnalysis = System.nanoTime()
      val count_* = FunctionCall(SpecialFunctions.StarFunc("count"), Nil)(NoPosition, NoPosition)
      val untypedSelectedExpressions = groupBy.getOrElse(Nil) :+ count_*
      val names = AliasAnalysis(Selection(None, None, untypedSelectedExpressions.map(SelectedExpression(_, None)))).expressions.keys.toSeq
      val afterAliasAnalysis = System.nanoTime()

      log.trace("alias analysis took {}ms", ns2ms(afterAliasAnalysis - beforeAliasAnalysis))

      val typedSelectedExpressions = checkedGroupBy.getOrElse(Nil) :+ typechecker(count_*, Map.empty)

      (names, typedSelectedExpressions)
    } else { // ok, no group by...
      log.debug("It is not grouped; selecting *")

      val beforeAliasAnalysis = System.nanoTime()
      val names = AliasAnalysis(Selection(None, Some(StarSelection(Nil)), Nil)).expressions.keys.toSeq
      val afterAliasAnalyis = System.nanoTime()

      log.trace("alias analysis took {}ms", ns2ms(afterAliasAnalyis - beforeAliasAnalysis))

      val typedSelectedExpressions = names.map { column =>
        typed.ColumnRef(column, ctx.schema(column))(NoPosition)
      }

      (names, typedSelectedExpressions)
    }

    finishAnalysis(
      isGrouped,
      OrderedMap(names.zip(typedSelectedExpressions) : _*),
      checkedWhere,
      checkedGroupBy,
      checkedHaving,
      checkedOrderBy,
      limit,
      offset,
      search)
  }

  def analyzeWithSelection(query: Select)(implicit ctx: DatasetContext[Type]): Analysis = {
    log.debug("There is a selection; typechecking all parts")

    val typechecker = new Typechecker(typeInfo, functionInfo)

    val t0 = System.nanoTime()
    val aliasAnalysis = AliasAnalysis(query.selection)
    val t1 = System.nanoTime()
    val typedAliases = aliasAnalysis.evaluationOrder.foldLeft(Map.empty[ColumnName, Expr]) { (acc, alias) =>
      acc + (alias -> typechecker(aliasAnalysis.expressions(alias), acc))
    }
    val outputs = OrderedMap(aliasAnalysis.expressions.keys.map { k => k -> typedAliases(k) }.toSeq : _*)
    val t2 = System.nanoTime()
    val checkedWhere = query.where.map(typechecker(_, typedAliases))
    val t3 = System.nanoTime()
    val checkedGroupBy = query.groupBy.map(_.map(typechecker(_, typedAliases)))
    val t4 = System.nanoTime()
    val checkedHaving = query.having.map(typechecker(_, typedAliases))
    val t5 = System.nanoTime()
    val checkedOrderBy = query.orderBy.map { obs => obs.zip(obs.map { ob => typechecker(ob.expression, typedAliases) }) }
    val t6 = System.nanoTime()
    val isGrouped = aggregateChecker(
      outputs.values.toSeq,
      checkedWhere,
      checkedGroupBy,
      checkedHaving,
      checkedOrderBy.getOrElse(Nil).map(_._2))
    val t7 = System.nanoTime()

    if(log.isTraceEnabled) {
      log.trace("alias analysis took {}ms", ns2ms(t1 - t0))
      log.trace("typechecking aliases took {}ms", ns2ms(t2 - t1))
      log.trace("typechecking WHERE took {}ms", ns2ms(t3 - t2))
      log.trace("typechecking GROUP BY took {}ms", ns2ms(t4 - t3))
      log.trace("typechecking HAVING took {}ms", ns2ms(t5 - t4))
      log.trace("typechecking ORDER BY took {}ms", ns2ms(t6 - t5))
      log.trace("checking for aggregation took {}ms", ns2ms(t7 - t6))
    }

    finishAnalysis(isGrouped, outputs, checkedWhere, checkedGroupBy, checkedHaving, checkedOrderBy, query.limit, query.offset, query.search)
  }

  def finishAnalysis(isGrouped: Boolean,
                     output: OrderedMap[ColumnName, Expr],
                     where: Option[Expr],
                     groupBy: Option[Seq[Expr]],
                     having: Option[Expr],
                     orderBy: Option[Seq[(OrderBy, Expr)]],
                     limit: Option[BigInt],
                     offset: Option[BigInt],
                     search: Option[String]): Analysis =
  {
    // todo: check that the types of the order-bys are Orderable

    SoQLAnalysis(
      isGrouped,
      output,
      where,
      groupBy,
      having,
      orderBy.map { obs => obs.map { case (ob, e) => typed.OrderBy(e, ob.ascending, ob.nullLast) } },
      limit,
      offset,
      search)
  }
}

case class SoQLAnalysis[ColumnId, Type](isGrouped: Boolean,
                                        selection: OrderedMap[ColumnName, typed.CoreExpr[ColumnId, Type]],
                                        where: Option[typed.CoreExpr[ColumnId, Type]],
                                        groupBy: Option[Seq[typed.CoreExpr[ColumnId, Type]]],
                                        having: Option[typed.CoreExpr[ColumnId, Type]],
                                        orderBy: Option[Seq[typed.OrderBy[ColumnId, Type]]],
                                        limit: Option[BigInt],
                                        offset: Option[BigInt],
                                        search: Option[String]) {
  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId): SoQLAnalysis[NewColumnId, Type] =
    copy(
      selection = selection.mapValues(_.mapColumnIds(f)),
      where = where.map(_.mapColumnIds(f)),
      groupBy = groupBy.map(_.map(_.mapColumnIds(f))),
      having = having.map(_.mapColumnIds(f)),
      orderBy = orderBy.map(_.map(_.mapColumnIds(f)))
    )
}
