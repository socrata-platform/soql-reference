package com.socrata.soql

import com.socrata.soql.exceptions._
import com.socrata.soql.functions.MonomorphicFunction

import scala.util.parsing.input.{NoPosition, Position}
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.aggregates.AggregateChecker
import com.socrata.soql.ast._
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typechecker._
import com.socrata.soql.environment._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.typed.{CoreExpr, NoContext, Qualifier}

class SoQLAnalyzer[Type](typeInfo: TypeInfo[Type],
                         functionInfo: FunctionInfo[Type],
                         parserParameters: AbstractParser.Parameters = AbstractParser.defaultParameters)
{
  type Analysis = SoQLAnalysis[ColumnName, Type]
  type Expr = typed.CoreExpr[ColumnName, Type]
  type Qualifier = String
  type AnalysisContext = Map[Qualifier, DatasetContext[Type]]

  val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLAnalyzer[_]])

  def ns2ms(ns: Long) = ns / 1000000

  val aggregateChecker = new AggregateChecker[Type]

  /** Turn a SoQL SELECT statement into a sequence of typed `Analysis` objects.
    *
    * @param query The SELECT to parse and analyze
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous
    * @return The analysis of the query */
  def analyzeFullQuery(query: String)(implicit ctx: AnalysisContext): Seq[Analysis] = {
    log.debug("Analyzing full query {}", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).selectStatement(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))
    analyze(parsed)
  }

  def analyze(selects: List[Select])(implicit ctx: AnalysisContext): List[Analysis] = {
    selects.headOption match {
      case Some(firstSelect) =>
        val firstAnalysis = analyzeWithSelection(firstSelect)(ctx)
        selects.tail.scanLeft(firstAnalysis)(analyzeInOuterSelectionContext(ctx))
      case None =>
        Nil
    }
  }

  // TODO: refinements?....
  def analyzeFrom(from: From)(implicit ctx: AnalysisContext): typed.From[ColumnName, Type] = from match {
    case From(TableName(name), refs, alias) => typed.From(typed.TableName(name), analyze(refs), alias)
    case From(bs: BasedSelect, refs, alias) => typed.From(analyzeBasedSelect(bs), analyze(refs), alias)
    case From(NoContext, refs, alias) => typed.From(new NoContext, analyze(refs), alias)
  }

  /** Turn a simple SoQL SELECT statement into an `Analysis` object.
    *
    * @param query The SELECT to parse and analyze
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous
    * @return The analysis of the query */
  def analyzeUnchainedQuery(query: String)(implicit ctx: AnalysisContext): Analysis = {
    log.debug("Analyzing full unchained query {}", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).unchainedSelectStatement(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))

    analyzeWithSelection(parsed)(ctx)
  }

  def analyzeFrom(query: String)(implicit ctx: AnalysisContext): typed.From[ColumnName, Type] = {
    log.debug("Analyzing from", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).standaloneFrom(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))

    analyzeFrom(parsed)(ctx)
  }

  private def baseAnalysis(implicit ctx: AnalysisContext) = {
    val selections = ctx.map { case (qual, dsCtx) =>
      val q = if (qual == TableName.PrimaryTable.name) None else Some(qual) // TODO (selectRework): table alias?
      dsCtx.schema.transform(typed.ColumnRef(q, _, _)(NoPosition))
    }
    // TODO: Enhance resolution of column name conflict from different tables in chained SoQLs
    val mergedSelection: OrderedMap[ColumnName, CoreExpr[ColumnName, Type]] = selections.reduce(_ ++ _)
    SimpleSoQLAnalysis.simpleAnalysis[ColumnName, Type].copy(selection = mergedSelection)
  }

  /** Turn framents of a SoQL SELECT statement into a typed `Analysis` object.  If no `selection` is provided,
    * one is generated based on whether the rest of the parameters indicate an aggregate query or not.  If it
    * is not an aggregate query, a selection list equivalent to `*` (i.e., every non-system column) is generated.
    * If it is an aggregate query, a selection list made up of the expressions from `groupBy` (if provided) together
    * with "`count(*)`" is generated.
    *
    * @param distinct   Reduce identical tuples into one.
    * @param selection  A selection list.
    * @param join       A join list.
    * @param where      An expression to be used as the query's WHERE clause.
    * @param groupBy    A comma-separated list of expressions to be used as the query's GROUP BY cluase.
    * @param having     An expression to be used as the query's HAVING clause.
    * @param orderBy    A comma-separated list of expressions and sort-order specifiers to be uesd as the query's ORDER BY clause.
    * @param limit      A non-negative integer to be used as the query's LIMIT parameter
    * @param offset     A non-negative integer to be used as the query's OFFSET parameter
    * @param sourceFrom The analysis-chain of the query this query is based upon, if applicable.
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous.
    * @return The analysis of the query.  Note this should be appended to `sourceFrom` to form a full query-chain. */
  def analyzeSplitQuery(distinct: Boolean,
                        selection: Option[String],
                        join: Option[String],
                        where: Option[String],
                        groupBy: Option[String],
                        having: Option[String],
                        orderBy: Option[String],
                        limit: Option[String],
                        offset: Option[String],
                        search: Option[String],
                        sourceFrom: Seq[Analysis] = Nil)(implicit ctx: AnalysisContext): Analysis = {
    log.debug("analyzing split query")

    val p = new Parser(parserParameters)

    val lastQuery: Analysis =
      if(sourceFrom.isEmpty) baseAnalysis
      else sourceFrom.last

    def dispatch(distinct: Boolean, selection: Option[Selection],
                 joins: List[Join],
                 where: Option[Expression], groupBy: List[Expression], having: Option[Expression], orderBy: List[OrderBy], limit: Option[BigInt], offset: Option[BigInt], search: Option[String]) =
      selection match {
        case None => analyzeNoSelectionInOuterSelectionContext(lastQuery, distinct, joins, where, groupBy, having, orderBy, limit, offset, search)
        case Some(s) => analyzeInOuterSelectionContext(ctx)(lastQuery, Select(distinct, s, joins, where, groupBy, having, orderBy, limit, offset, search))
      }

    dispatch(
      distinct,
      selection.map(p.selection),
      join.map(p.joins).getOrElse(Nil),
      where.map(p.expression),
      groupBy.map(p.groupBys).getOrElse(Nil),
      having.map(p.expression),
      orderBy.map(p.orderings).getOrElse(Nil),
      limit.map(p.limit),
      offset.map(p.offset),
      search.map(p.search)
    )
  }

  def analyzeNoSelectionInOuterSelectionContext(lastQuery: Analysis,
                                                distinct: Boolean,
                                                joins: List[Join],
                                                where: Option[Expression],
                                                groupBy: List[Expression],
                                                having: Option[Expression],
                                                orderBy: List[OrderBy],
                                                limit: Option[BigInt],
                                                offset: Option[BigInt],
                                                search: Option[String]): Analysis = {
    implicit val fakeCtx = contextFromAnalysis(lastQuery)
    analyzeNoSelection(distinct, joins, where, groupBy, having, orderBy, limit, offset, search)
  }

  private def getNestedTableName(from: From): String = {
    from match {
      case From(bs: BasedSelect, _, _) =>
        bs.from match {
          case From(TableName(name), _, _) => name
        }
      case From(TableName(name), _, alias) => alias.getOrElse(name)
    }
  }


  // TODO: can we just make joins here instead of only creating a context?
  private def joinCtx(joins: List[Join])(implicit ctx: AnalysisContext) = {
    joins.filterNot(j => SimpleSelect.isSimple(j.from)).map { j: Join =>
      val joinCtx: Map[Qualifier, DatasetContext[Type]] = ctx +
        (TableName.PrimaryTable.name -> ctx(getNestedTableName(j.from)))
      val from = analyzeFrom(j.from)(joinCtx)
      contextFromAnalysis(j.from.alias.getOrElse(
        throw new BadParse("Sub-query join must use alias.  Hint: JOIN (SELECT ...) AS T1", j.on.position)),
        from.source.asInstanceOf[BasedSoQLAnalysis[ColumnName, Type]].decontextualized)
    }
  }

  def analyzeNoSelection(distinct: Boolean,
                         join: List[Join],
                         where: Option[Expression],
                         groupBy: List[Expression],
                         having: Option[Expression],
                         orderBy: List[OrderBy],
                         limit: Option[BigInt],
                         offset: Option[BigInt],
                         search: Option[String])(implicit ctx: AnalysisContext): Analysis = {
    log.debug("No selection; doing typechecking of the other parts then deciding what to make the selection")

    val ctxFromJoins = joinCtx(join)
    val ctxWithJoins = ctx ++ ctxFromJoins

    // ok, so the only tricky thing here is the selection itself.  Since it isn't provided, there are two cases:
    //   1. If there are groupBys, having, or aggregates in orderBy, then selection should be the equivalent of
    //      selecting the group-by clauses together with "count(*)".
    //   2. Otherwise, it should be the equivalent of selecting "*".
    val typechecker = new Typechecker(typeInfo, functionInfo)(ctxWithJoins)
    val subscriptConverter = new SubscriptConverter(typeInfo, functionInfo)

    // Rewrite subscript before typecheck
    val typecheck = subscriptConverter andThen (e => typechecker(e, Map.empty))

    val t0 = System.nanoTime()
    val checkedWhere = where.map(typecheck)
    val t1 = System.nanoTime()
    val checkedGroupBy = groupBy.map(typecheck)
    val t2 = System.nanoTime()
    val checkedHaving = having.map(typecheck)
    val t3 = System.nanoTime()
    val checkedOrderBy = orderBy.zip(orderBy.map(ob => typecheck(ob.expression)))
    val t4 = System.nanoTime()
    val isGrouped = aggregateChecker(Nil, checkedWhere, checkedGroupBy, checkedHaving, checkedOrderBy.map(_._2))
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
      val untypedSelectedExpressions = groupBy :+ count_*
      val names = AliasAnalysis(Selection(None, Seq.empty, untypedSelectedExpressions.map(SelectedExpression(_, None)))).expressions.keys.toSeq
      val afterAliasAnalysis = System.nanoTime()

      log.trace("alias analysis took {}ms", ns2ms(afterAliasAnalysis - beforeAliasAnalysis))

      val typedSelectedExpressions = checkedGroupBy :+ typechecker(count_*, Map.empty)

      (names, typedSelectedExpressions)
    } else { // ok, no group by...
      log.debug("It is not grouped; selecting *")

      val beforeAliasAnalysis = System.nanoTime()
      val names = AliasAnalysis(Selection(None, Seq(StarSelection(None, Nil)), Nil)).expressions.keys.toSeq
      val afterAliasAnalyis = System.nanoTime()

      log.trace("alias analysis took {}ms", ns2ms(afterAliasAnalyis - beforeAliasAnalysis))

      val typedSelectedExpressions = names.map { column =>
        typed.ColumnRef(None, column, ctx(TableName.PrimaryTable.name).schema(column))(NoPosition)
      }

      (names, typedSelectedExpressions)
    }

    // TODO (selectRework): dedupe
    val checkedJoin = join.map { j: Join =>
      val sourceName = j.from match {
        case From(TableName(name), _, _) => name
        case f => f.alias.get
      }
      val joinCtx = ctx + (TableName.PrimaryTable.name -> ctx(sourceName)) // overwriting primary table but keeping other info?
      val from = analyzeFrom(j.from)(joinCtx)
      typed.Join(j.typ, from, typecheck(j.on))
    }

    finishAnalysis(
      isGrouped,
      distinct,
      OrderedMap(names.zip(typedSelectedExpressions): _*),
      checkedJoin,
      checkedWhere,
      checkedGroupBy,
      checkedHaving,
      checkedOrderBy,
      limit,
      offset,
      search)
  }

  def analyzeInOuterSelectionContext(initialCtx: AnalysisContext)(lastQuery: Analysis, query: Select): Analysis = {
    val prevCtx = contextFromAnalysis(lastQuery)
    val nextCtx = prevCtx ++ (initialCtx - TableName.PrimaryTable.name)
    analyzeWithSelection(query)(nextCtx)
  }

  def contextFromAnalysis(a: Analysis): AnalysisContext = {
    val ctx = new DatasetContext[Type] {
      override val schema: OrderedMap[ColumnName, Type] = a.selection.mapValues(_.typ)
    }
    Map(TableName.PrimaryTable.name -> ctx)
  }

  private def contextFromAnalysis(qualifier: Qualifier, a: Analysis) = {
    val ctx = new DatasetContext[Type] {
      override val schema: OrderedMap[ColumnName, Type] = a.selection.mapValues(_.typ)
    }
    qualifier -> ctx
  }

  def analyzeBasedSelect(basedSelect: BasedSelect)(implicit ctx: AnalysisContext): BasedSoQLAnalysis[ColumnName, Type] = {
    val from = analyzeFrom(basedSelect.from) // TODO(?): include context information from this for analysis of basedSelect?
    analyze(List(basedSelect.decontextualized)).head.contextualize(from) // make this better
  }

  def analyzeWithSelection(query: Select)(implicit ctx: AnalysisContext): Analysis = {
    log.debug("There is a selection; typechecking all parts")

    val ctxFromJoins = joinCtx(query.join)
    val ctxWithJoins = ctx ++ ctxFromJoins
    val typechecker = new Typechecker(typeInfo, functionInfo)(ctxWithJoins)

    val subscriptConverter = new SubscriptConverter(typeInfo, functionInfo)

    val t0 = System.nanoTime()
    val aliasAnalysis = AliasAnalysis(query.selection)
    val t1 = System.nanoTime()
    val typedAliases = aliasAnalysis.evaluationOrder.foldLeft(Map.empty[ColumnName, Expr]) { (acc, alias) =>
      acc + (alias -> typechecker(subscriptConverter(aliasAnalysis.expressions(alias)), acc))
    }

    // Rewrite subscript before typecheck
    val typecheck = subscriptConverter andThen (e => typechecker(e, typedAliases))

    val checkedJoin = query.join.map { j: Join =>
      val joinCtx = ctx + (TableName.PrimaryTable.name -> ctx(getNestedTableName(j.from))) // overwriting primary table but keeping other info?
      val from = analyzeFrom(j.from)(joinCtx)
      typed.Join(j.typ, from, typecheck(j.on))
    }

    val outputs = OrderedMap(aliasAnalysis.expressions.keys.map { k => k -> typedAliases(k) }.toSeq : _*)
    val t2 = System.nanoTime()
    val checkedWhere = query.where.map(typecheck)
    val t3 = System.nanoTime()
    val checkedGroupBy = query.groupBy.map(typecheck)
    val t4 = System.nanoTime()
    val checkedHaving = query.having.map(typecheck)
    val t5 = System.nanoTime()
    val orderBy = query.orderBy
    val checkedOrderBy = orderBy.zip(orderBy.map(ob => typecheck(ob.expression)))
    val t6 = System.nanoTime()
    val isGrouped = aggregateChecker(
      outputs.values.toSeq,
      checkedWhere,
      checkedGroupBy,
      checkedHaving,
      checkedOrderBy.map(_._2)
    )

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

    finishAnalysis(isGrouped, query.distinct,
                   outputs, checkedJoin, checkedWhere, checkedGroupBy, checkedHaving, checkedOrderBy,
                   query.limit, query.offset, query.search)
  }

  def finishAnalysis(isGrouped: Boolean,
                     distinct: Boolean,
                     output: OrderedMap[ColumnName, Expr],
                     join: List[typed.Join[ColumnName, Type]],
                     where: Option[Expr],
                     groupBy: List[Expr],
                     having: Option[Expr],
                     orderBy: List[(OrderBy, Expr)],
                     limit: Option[BigInt],
                     offset: Option[BigInt],
                     search: Option[String]): Analysis =
  {
    def check(items: TraversableOnce[Expr], pred: Type => Boolean, onError: (TypeName, Position) => Throwable) {
      for(item <- items if !pred(item.typ)) throw onError(typeInfo.typeNameFor(item.typ), item.position)
    }

    check(where, typeInfo.isBoolean, NonBooleanWhere)
    check(groupBy, typeInfo.isGroupable, NonGroupableGroupBy)
    check(having, typeInfo.isBoolean, NonBooleanHaving)
    check(orderBy.map(_._2), typeInfo.isOrdered, UnorderableOrderBy)

    SoQLAnalysis(
      isGrouped,
      distinct,
      output,
      join,
      where,
      groupBy,
      having,
      orderBy.map { case (ob, e) => typed.OrderBy(e, ob.ascending, ob.nullLast) },
      limit,
      offset,
      search)
  }
}

/**
  * @param isGrouped true iff there is a group by or aggregation function applied.  Can be derived from the selection
  *                  and the groupBy
  */
case class SoQLAnalysis[ColumnId, Type](isGrouped: Boolean,
                                        distinct: Boolean,
                                        selection: OrderedMap[ColumnName, typed.CoreExpr[ColumnId, Type]],
                                        joins: List[typed.Join[ColumnId, Type]],
                                        where: Option[typed.CoreExpr[ColumnId, Type]],
                                        groupBy: List[typed.CoreExpr[ColumnId, Type]],
                                        having: Option[typed.CoreExpr[ColumnId, Type]],
                                        orderBy: List[typed.OrderBy[ColumnId, Type]],
                                        limit: Option[BigInt],
                                        offset: Option[BigInt],
                                        search: Option[String]) {

  def contextualize(from: typed.From[ColumnId, Type]) = {
    BasedSoQLAnalysis(isGrouped, distinct, selection, from, joins, where, groupBy, having, orderBy, limit, offset, search)
  }

  /*
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): SoQLAnalysis[NewColumnId, Type] =
    copy(
      selection = selection.mapValues(_.mapColumnIds(f)),
      joins = joins.map { join =>

        def joinMap(c: ColumnId, q: Qualifier): NewColumnId = {
          val qualifierForJoin = q.orElse(join.from.source match {
            case typed.TableName(name)[_, _] => Some(name)
            case _ => None
          })
          f(c, qualifierForJoin)
        }

        val remappedJoin = join.from match {
          case typed.From[_, _](bs: BasedSoqlAnalysis[ColumnId, Type], refs, alias) => From(bs.mapColumnIds(mapColumnIds(joinMap)), refs.mapColumnIds(joinMap), alias)
          case typed.From[_, _](_, refs, alias) => From(bs.mapColumnIds(mapColumnIds(joinMap)), refs.mapColumnIds(joinMap), alias)
        typed.Join(join.typ, remappedJoin, join.alias, join.on.mapColumnIds(f))
      }},
      where = where.map(_.mapColumnIds(f)),
      groupBy = groupBy.map(_.map(_.mapColumnIds(f))),
      having = having.map(_.mapColumnIds(f)),
      orderBy = orderBy.map(_.map(_.mapColumnIds(f)))
    )

  def mapColumnIds[NewColumnId](qColumnIdNewColumnIdMap: Map[(ColumnId, Qualifier), NewColumnId],
                                qColumnNameToQColumnId: (Qualifier, ColumnName) => (ColumnId, Qualifier),
                                columnNameToNewColumnId: ColumnName => NewColumnId,
                                columnIdToNewColumnId: ColumnId => NewColumnId): SoQLAnalysis[NewColumnId, Type] = {
    val newColumnsFromJoin = joins.toSeq.flatten.filter(x => !SimpleSoQLAnalysis.isSimple(x.from)).flatMap { j =>
      j.tableLike.last.selection.map { case (columnName, _) =>
        qColumnNameToQColumnId(j.alias, columnName) -> columnNameToNewColumnId(columnName)
      }}.toMap

    val qColumnIdNewColumnIdWithJoinsMap = qColumnIdNewColumnIdMap ++ newColumnsFromJoin

    copy(
      selection = selection.mapValues(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      joins = joins.map { joins => joins.map { j =>
        val analysesInColIds = j.from.foldLeft(Seq.empty[SoQLAnalysis[NewColumnId, Type]]) { (convertedAnalyses, analysis) =>
          val joinMap =
            convertedAnalyses.lastOption match {
              case None =>
                qColumnIdNewColumnIdMap.foldLeft(Map.empty[(ColumnId, Qualifier), NewColumnId]) { (acc, kv) =>
                  val ((cid, qual), ncid) = kv
                  if (qual == j.from.head.from) acc + ((cid, None) -> ncid)
                  else acc
                }
              case Some(prevAnalysis) =>
                prevAnalysis.selection.foldLeft(qColumnIdNewColumnIdMap) { (acc, selCol) =>
                  val (colName, expr) = selCol
                  acc + (qColumnNameToQColumnId(None, colName) -> columnNameToNewColumnId(colName))
                }
            }

          val a = analysis.mapColumnIds(joinMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
          convertedAnalyses :+ a
        }

        val remappedJoin = analysesInColIds
        typed.Join(j.typ, remappedJoin, j.alias, j.on.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap)))
      }},
      where = where.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      groupBy = groupBy.map(_.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap)))),
      having = having.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      orderBy = orderBy.map(_.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))))
    )
  }
    */
}

case class BasedSoQLAnalysis[ColumnId, Type](isGrouped: Boolean,
                                            distinct: Boolean,
                                            selection: OrderedMap[ColumnName, typed.CoreExpr[ColumnId, Type]],
                                            from: typed.From[ColumnId, Type],
                                            joins: List[typed.Join[ColumnId, Type]],
                                            where: Option[typed.CoreExpr[ColumnId, Type]],
                                            groupBy: List[typed.CoreExpr[ColumnId, Type]],
                                            having: Option[typed.CoreExpr[ColumnId, Type]],
                                            orderBy: List[typed.OrderBy[ColumnId, Type]],
                                            limit: Option[BigInt],
                                            offset: Option[BigInt],
                                            search: Option[String]) extends typed.TableSource[ColumnId, Type] {

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): BasedSoQLAnalysis[NewColumnId, Type] = {
    ???
  }

  def decontextualized =
    SoQLAnalysis(isGrouped, distinct, selection, joins, where, groupBy, having, orderBy, limit, offset, search)

}

object SoQLAnalysis {
  def merge[T](andFunction: MonomorphicFunction[T], stages: Seq[SoQLAnalysis[ColumnName, T]]): Seq[SoQLAnalysis[ColumnName, T]] =
    new Merger(andFunction).merge(stages)
}

object SimpleSoQLAnalysis {
  def simpleAnalysis[ColumnId, Type] = {
    SoQLAnalysis[ColumnId, Type](
      isGrouped = false,
       distinct = false,
       selection = OrderedMap.empty,
       joins = Nil,
       where = None,
       groupBy = Nil,
       having = None,
       orderBy = Nil,
       limit = None,
       offset = None,
       search = None
    )
  }

  /**
    * Simple SoQLAnalysis is a soql analysis created by a join where a sub-query is not used like "JOIN @aaaa-aaaa"
    */
  /*
  def isSimple[ColumnId, Type](a: SoQLAnalysis[ColumnId, Type]): Boolean = {
    a.selection.keys.isEmpty && a.from.nonEmpty
  }

  def isSimple[ColumnId, Type](a: Seq[SoQLAnalysis[ColumnId, Type]]): Boolean = {
    a.nonEmpty && isSimple(a.last)
  }

  def asSoQL[ColumnId, Type](a: Seq[SoQLAnalysis[ColumnId, Type]]): Option[String] = {
    if (isSimple(a)) {
      a.last.from
    } else {
      None
    }
  }
  */
}


// TODO (selectRework): removed guards on aFrom == bFrom... SoQLAnalysis no longer has a from... implications?
private class Merger[T](andFunction: MonomorphicFunction[T]) {
  type Analysis = SoQLAnalysis[ColumnName, T]
  type Expr = typed.CoreExpr[ColumnName, T]

  def merge(stages: Seq[Analysis]): Seq[Analysis] =
    stages.drop(1).foldLeft(stages.take(1).toList) { (acc, nextStage) =>
      tryMerge(acc.head /* acc cannot be empty */, nextStage) match {
        case Some(merged) => merged :: acc.tail
        case None => nextStage :: acc
      }
    }.reverse

  // Currently a search on the second query prevents merging, as its meaning ought to
  // be "search the output of the first query" rather than "search the underlying
  // dataset".  Unfortunately this means "select :*,*" isn't a left-identity of merge
  // for a query that contains a search.
  private def tryMerge(a: Analysis, b: Analysis): Option[Analysis] = (a, b) match {
    case (SoQLAnalysis(aIsGroup, false, aSelect, Nil, aWhere, aGroup, aHaving, aOrder, aLim, aOff, aSearch),
          SoQLAnalysis(false, false, bSelect, bJoin, None, Nil, None, Nil, bLim, bOff, None)) if
          // Do not merge when the previous soql is grouped and the next soql has joins
          // select g, count(x) as cx group by g |> select g, cx, @b.a join @b on @b.g=g
          // Newly introduced columns from joins cannot be merged and brought in w/o grouping and aggregate functions.
          !(aIsGroup && bJoin.nonEmpty) =>
      // we can merge a change of only selection and limit + offset onto anything
      val (newLim, newOff) = Merger.combineLimits(aLim, aOff, bLim, bOff)
      Some(SoQLAnalysis(isGrouped = aIsGroup,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = bJoin,
                        where = aWhere,
                        groupBy = aGroup,
                        having = aHaving,
                        orderBy = aOrder,
                        limit = newLim,
                        offset = newOff,
                        search = aSearch))
    case (SoQLAnalysis(false, false, aSelect, Nil, aWhere, Nil, None, aOrder, None, None, aSearch),
          SoQLAnalysis(false, false, bSelect, bJoin, bWhere, Nil, None, bOrder, bLim, bOff, None)) =>
      // Can merge a change of filter or order only if no window was specified on the left
      Some(SoQLAnalysis(isGrouped = false,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = bJoin,
                        where = mergeWhereLike(aSelect, aWhere, bWhere),
                        groupBy = Nil,
                        having = None,
                        orderBy = mergeOrderBy(aSelect, aOrder, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (SoQLAnalysis(false, false, aSelect, Nil, aWhere, Nil,     None,    _,      None, None, aSearch),
          SoQLAnalysis(true, false, bSelect, bJoin, bWhere, bGroupBy, bHaving, bOrder, bLim, bOff, None)) =>
      // an aggregate on a non-aggregate
      Some(SoQLAnalysis(isGrouped = true,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = bJoin,
                        where = mergeWhereLike(aSelect, aWhere, bWhere),
                        groupBy = mergeGroupBy(aSelect, bGroupBy),
                        having = mergeWhereLike(aSelect, None, bHaving),
                        orderBy = mergeOrderBy(aSelect, Nil, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (SoQLAnalysis(true,  false, aSelect, Nil, aWhere, aGroupBy, aHaving, aOrder, None, None, aSearch),
          SoQLAnalysis(false, false, bSelect, Nil, bWhere, Nil,     None,    bOrder, bLim, bOff, None)) =>
      // a non-aggregate on an aggregate -- merge the WHERE of the second with the HAVING of the first
      Some(SoQLAnalysis(isGrouped = true,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = Nil,
                        where = aWhere,
                        groupBy = aGroupBy,
                        having = mergeWhereLike(aSelect, aHaving, bWhere),
                        orderBy = mergeOrderBy(aSelect, aOrder, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (_, _) =>
      None
  }

  private def mergeSelection(a: OrderedMap[ColumnName, Expr],
                             b: OrderedMap[ColumnName, Expr]): OrderedMap[ColumnName, Expr] =
    // ok.  We don't need anything from A, but columnRefs in b's expr that refer to a's values need to get substituted
    b.mapValues(replaceRefs(a, _))

  private def mergeOrderBy(aliases: OrderedMap[ColumnName, Expr],
                           obA: List[typed.OrderBy[ColumnName, T]],
                           obB: List[typed.OrderBy[ColumnName, T]]): List[typed.OrderBy[ColumnName, T]] =
    (obA, obB) match {
      case (Nil, Nil) => Nil
      case (a, Nil) => a
      case (Nil, b) => b.map { ob => ob.copy(expression = replaceRefs(aliases, ob.expression)) }
      case (a, b) => b.map { ob => ob.copy(expression = replaceRefs(aliases, ob.expression)) } ++ a
    }

  private def mergeWhereLike(aliases: OrderedMap[ColumnName, Expr],
                             a: Option[Expr],
                             b: Option[Expr]): Option[Expr] =
    (a, b) match {
      case (None, None) => None
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(replaceRefs(aliases, b))
      case (Some(a), Some(b)) => Some(typed.FunctionCall(andFunction, List(a, replaceRefs(aliases, b)))(NoPosition, NoPosition))
    }

  private def mergeGroupBy(aliases: OrderedMap[ColumnName, Expr], gb: List[Expr]): List[Expr] =
    gb.map(replaceRefs(aliases, _))

  private def replaceRefs(a: OrderedMap[ColumnName, Expr],
                             b: Expr): Expr =
    b match {
      case cr@typed.ColumnRef(Some(q), c, t) =>
        cr
      case cr@typed.ColumnRef(None, c, t) =>
        a.getOrElse(c, cr)
      case tl: typed.TypedLiteral[T] =>
        tl
      case fc@typed.FunctionCall(f, params) =>
        typed.FunctionCall(f, params.map(replaceRefs(a, _)))(fc.position, fc.functionNamePosition)
    }
}

private object Merger {
  def combineLimits(aLim: Option[BigInt], aOff: Option[BigInt], bLim: Option[BigInt], bOff: Option[BigInt]): (Option[BigInt], Option[BigInt]) = {
    // ok, what we're doing here is basically finding the intersection of two segments of
    // the integers, where either segment may end at infinity.  Note that all the inputs are
    // non-negative (enforced by the parser).  This simplifies things somewhat
    // because we can assume that aOff + bOff > aOff
    require(aLim.forall(_ >= 0), "Negative aLim")
    require(aOff.forall(_ >= 0), "Negative aOff")
    require(bLim.forall(_ >= 0), "Negative bLim")
    require(bOff.forall(_ >= 0), "Negative bOff")

    (aLim, aOff, bLim, bOff) match {
      case (None, None, bl, bo) =>
        (bl, bo)
      case (None, Some(ao), bl, bo) =>
        // first is unbounded to the right
        (bl, Some(ao + bo.getOrElse(BigInt(0))))
      case (Some(al), ao, Some(bl), bo) =>
        // both are bound to the right
        val trueAOff = ao.getOrElse(BigInt(0))
        val trueBOff = bo.getOrElse(BigInt(0)) + trueAOff
        val trueAEnd = trueAOff + al
        val trueBEnd = trueBOff + bl

        val trueOff = trueBOff min trueAEnd
        val trueEnd = trueBEnd min trueAEnd
        val trueLim = trueEnd - trueOff

        (Some(trueLim), Some(trueOff))
      case (Some(al), ao, None, bo) =>
        // first is bound to the right but the second is not
        val trueAOff = ao.getOrElse(BigInt(0))
        val trueBOff = bo.getOrElse(BigInt(0)) + trueAOff

        val trueEnd = trueAOff + al
        val trueOff = trueBOff min trueEnd
        val trueLim = trueEnd - trueOff
        (Some(trueLim), Some(trueOff))
    }
  }
}
