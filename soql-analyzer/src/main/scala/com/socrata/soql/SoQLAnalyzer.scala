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
import com.socrata.soql.collection.{OrderedMap, OrderedSet, NonEmptySeq}
import com.socrata.soql.collection.SeqHelpers._
import Select._

/**
  * The type-checking version of [[com.socrata.soql.parsing.AbstractParser]]. Turns string soql statements into
  * Lists of Analysis objects (soql ast), while using type information about the tables to typecheck all
  * expressions.
  */
class SoQLAnalyzer[Type](typeInfo: TypeInfo[Type],
                         functionInfo: FunctionInfo[Type],
                         tableFinder: Set[ResourceName] => Map[ResourceName, DatasetContext[Type]],
                         parserParameters: AbstractParser.Parameters = AbstractParser.defaultParameters)
{
  /** The typed version of [[com.socrata.soql.ast.Select]] */
  type Analysis = SoQLAnalysis[Qualified[ColumnName], Type]
  type Expr = typed.CoreExpr[Qualified[ColumnName], Type]

  private type Universe = Map[ResourceName, DatasetContext[Type]]
  private case class Context(
    // The current source table for unqualified column names.
    primaryDataset: ResourceName,
    // The current table that unqualified column names _that are
    // columns_ (as opposed to aliases for computed values) refers
    // to.  If this is a `JoinTableRef` then its resourceName is
    // a key in `universe`; if this is `Primary` then `primaryDataset`
    // is a key in `universe`.
    implicitTableRef: TableRef with TableRef.Implicit,
    // The current "implicit schema" that is used to look up
    // unqualified column names.  Most, but not all, of the Exprs will
    // just be ColumnRefs.
    implicitSchema: OrderedMap[ColumnName, Expr],
    // This is all the actual physical tables we're working with in
    // this entire chained query.  It does not change while moving
    // through the query.
    universe: Universe,
    // The visible table names that can be referred to.  If a value
    // here is `PrimaryTableRef`, then the value's resourceName is a
    // key in `universe`.
    visible: Map[ResourceName, TableRef]
  )


  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLAnalyzer[_]])

  private def ns2ms(ns: Long) = ns / 1000000

  private val aggregateChecker = new AggregateChecker[Type]
  private val typechecker = new Typechecker(typeInfo, functionInfo)

  private def contextToSimpleSelection(ctx: DatasetContext[Type], tableRef: TableRef): OrderedMap[ColumnName, Expr] =
    ctx.schema.transformOrdered { (columnName, columnType) =>
      typed.ColumnRef(Qualified(tableRef, columnName), columnType)(NoPosition)
    }

  private def analysisToSimpleSelection(analysis: Analysis, tableRef: TableRef): OrderedMap[ColumnName, Expr] =
    analysis.selection.transformOrdered { (columnName, expr) =>
      typed.ColumnRef(Qualified(tableRef, columnName), expr.typ)(expr.position)
    }

  private def tablesOfInterest(context: ResourceName, tables: Set[ResourceName], primaryRef: TableRef with TableRef.PrimaryCandidate): Context = {
    val allTables = tables + context
    val universe = tableFinder(allTables)
    require(universe.keySet == allTables, "Table finder did not return the right set of tables")
    Context(context,
            primaryRef,
            contextToSimpleSelection(universe(context), primaryRef),
            universe,
            Map(context -> primaryRef))
  }

  /** Turn a SoQL SELECT statement into a sequence of typed `Analysis` objects.
    *
    * A List is returned, with one [[SoQLAnalysis]] per chained soql element. This is the typed version of
    * AbstractParser#selectStatement.
    *
    * the chained soql:
    *   "select id, a |> select a"
    * is equivalent to:
    *   "select a from (select id, a [from current view]) as alias"
    * and is represented as:
    *   List(analysis_id_a, analysis_id)
    *
    * @param query The SELECT to parse and analyze
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous
    * @return The analysis of the query*/
  def analyzeFullQuery(context: ResourceName, query: String, primaryRef: TableRef with TableRef.PrimaryCandidate = TableRef.Primary): NonEmptySeq[Analysis] = {
    log.debug("Analyzing full query {}", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).selectStatement(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))

    analyze(context, parsed, primaryRef)
  }

  def analyze(context: ResourceName, selects: NonEmptySeq[Select], primaryRef: TableRef with TableRef.PrimaryCandidate = TableRef.Primary): NonEmptySeq[Analysis] = {
    val result =
      analyzeChainInContext(
        tablesOfInterest(context, selects.iterator.map(_.allTableReferences).foldLeft(Set.empty[ResourceName])(_ union _), primaryRef),
        selects)
    SoQLAnalysis.assertSaneInputs(result, primaryRef)
    result
  }

  private def analyzeChainInContext(context: Context, selects: NonEmptySeq[Select]): NonEmptySeq[Analysis] = {
    selects.scanLeft1(sel => (context, analyzeInContext(context, sel))) { (ctxAnalysis, select) =>
      val (ctx, lastAnalysis) = ctxAnalysis
      val ref = ctx.implicitTableRef.next
      val newContext = Context(primaryDataset = ctx.primaryDataset,
                               implicitTableRef = ref,
                               implicitSchema = analysisToSimpleSelection(lastAnalysis, ref),
                               universe = context.universe,
                               visible = Map.empty)
      (newContext, analyzeInContext(newContext, select))
    }.map(_._2)
  }

  /** Turn a simple SoQL SELECT statement into an `Analysis` object.
    *
    * @param query The SELECT to parse and analyze
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous
    * @return The analysis of the query */
  def analyzeUnchainedQuery(context: ResourceName, query: String, primaryRef: TableRef with TableRef.PrimaryCandidate = TableRef.Primary): Analysis = {
    log.debug("Analyzing full unchained query {}", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).unchainedSelectStatement(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))

    analyzeInContext(tablesOfInterest(context, parsed.allTableReferences, primaryRef),
                     parsed)
  }

  // "partially analyzing" a join means analyzing the query part but
  // not the ON part.
  private case class PartiallyAnalyzedJoin(originalJoin: Join, analysis: NonEmptySeq[Analysis], outputTableRef: TableRef) {
    def outputTableIdentifier = originalJoin.outputTableIdentifier
    def name = originalJoin.name
  }

  private def partialAnalyzeJoin(universe: Universe, join: Join): PartiallyAnalyzedJoin = {
    log.debug("Partially analyzing join {} in {}", join : Any, universe)
    val (analysis, ref) =
      join.from match {
        case JoinTable(table, _, joinNum) =>
          val primary = TableRef.JoinPrimary(table.resourceName, joinNum)

          (NonEmptySeq(SoQLAnalysis(
                         primary,
                         false,
                         false,
                         contextToSimpleSelection(universe(table.resourceName), primary),
                         Nil,
                         None,
                         Nil,
                         None,
                         Nil,
                         None,
                         None,
                         None)), primary)
        case JoinSelect(from, innerAlias, subselects, _, joinNum) =>
          val fromRef = TableRef.JoinPrimary(from.resourceName, joinNum)

          val analysis =
            analyzeChainInContext(
              Context(from.resourceName,
                      implicitTableRef = fromRef,
                      implicitSchema = contextToSimpleSelection(universe(from.resourceName), fromRef),
                      universe = universe,
                      visible = innerAlias match {
                        case Some(a) => Map(a.resourceName -> fromRef)
                        case None => Map(from.resourceName -> fromRef)
                      }),
              subselects)
          (analysis, TableRef.Join(joinNum))
      }
    PartiallyAnalyzedJoin(join, analysis, ref)
  }

  private def analyzeInContext(baseContext: Context, query: Select): Analysis = {
    log.debug("There is a selection; typechecking all parts")

    // The base context is the context we inherit from our position in
    // the whole query (i.e., the primary dataset if we're the first
    // step in the top level chain, the FROM dataset if we're the
    // first step in a subselect chain, or the output of the previous
    // step if we're not the first step in a chain).  We need to
    // augment it with tables from joins located here, now.

    // time info is t_{previousthing}_{nextthing}
    val t_start_partialAnalysis = System.nanoTime()

    val partiallyAnalyzedJoins = query.joins.map(partialAnalyzeJoin(baseContext.universe, _))

    val paJoinsBySubselectId = partiallyAnalyzedJoins.map { paJoin =>
      paJoin.outputTableIdentifier -> paJoin
    }.toMap

    val t_partialAnalysis_fullContext = System.nanoTime()

    val fullContext =
      partiallyAnalyzedJoins.foldLeft(baseContext) { (ctx, partiallyAnalyzedJoin) =>
        val freshName = partiallyAnalyzedJoin.name
        log.debug("Adding {} to fullContext", freshName)
        if(ctx.visible.contains(freshName.resourceName)) {
          throw new DuplicateTableAlias(freshName.resourceName, freshName.position)
        } else {
          ctx.copy(visible = ctx.visible + (freshName.resourceName -> partiallyAnalyzedJoin.outputTableRef))
        }
      }

    log.debug("Full context visible: {}", fullContext.visible)

    val t_fullContext_aliasAnalysis = System.nanoTime()

    // ok so when we're doing alias-analysis, we need to tell it,
    // given a (source-code) qualifier, what are the columns in that
    // table.
    val aliasAnalysisContext: Map[Option[ResourceName], OrderedSet[ColumnName]] = locally {
      val base =
        baseContext.implicitTableRef match {
          case TableRef.Primary =>
            Map(None -> baseContext.implicitSchema.keySet,
                Some(baseContext.primaryDataset) -> baseContext.implicitSchema.keySet)
          case TableRef.JoinPrimary(rn, _) =>
            Map(None -> baseContext.implicitSchema.keySet,
                Some(rn) -> baseContext.implicitSchema.keySet)
          case TableRef.PreviousChainStep(_, _) =>
            Map(None -> baseContext.implicitSchema.keySet)
        }

      base ++ partiallyAnalyzedJoins.map { paJoin =>
        Some(paJoin.name.resourceName) -> paJoin.analysis.last.selection.keySet
      }
    }
    val aliasAnalysis = AliasAnalysis(aliasAnalysisContext, query.selection)

    val t_aliasAnalysis_selectionTypechecking = System.nanoTime()

    // ok so here we're goin' a little CRAZY
    //
    // so
    //
    // typechecking aliases needs to know the whole context, joins and
    // everything including other aliases, though we know there's
    // _some_ order (supplied by alias analysis) in which we can
    // typecheck them without cycles.  This is that.  The output will
    // be the typechecked selections together with a context that
    // includes all aliases.

    log.debug("Join IDs: {}", paJoinsBySubselectId.keys)

    val (typecheckerContext, typecheckedSelections) = locally {
      val initialTypecheckerContext =
        Typechecker.Ctx(fullContext.implicitSchema, fullContext.visible.mapValues { tableRef =>
                          tableRef match {
                            case ref@TableRef.Primary =>
                              contextToSimpleSelection(fullContext.universe(fullContext.primaryDataset), ref)
                            case ref@TableRef.JoinPrimary(tn, _) =>
                              contextToSimpleSelection(fullContext.universe(tn), ref)
                            case ref@TableRef.Join(i) =>
                              analysisToSimpleSelection(paJoinsBySubselectId(i).analysis.last, ref)
                            case ref@TableRef.PreviousChainStep(_, _) =>
                              fullContext.implicitSchema.transformOrdered { (columnName, expr) =>
                                typed.ColumnRef(Qualified(ref, columnName), expr.typ)(expr.position)
                              }
                          }
                        })

      // "unordered" because while they are, in fact, in a particular
      // order (the alias evaluation order) they're not in the order we
      // care about (which is left-to-right).
      val (typecheckerContext, unorderedTypecheckedSelections) = aliasAnalysis.evaluationOrder.mapAccum(initialTypecheckerContext) { (acc, alias) =>
        val typechecked = typechecker(aliasAnalysis.expressions(alias), acc)
        (acc.copy(primarySchema = acc.primarySchema + (alias -> typechecked)), alias -> typechecked)
      }

      val unorderedTypecheckedSelectionsMap = unorderedTypecheckedSelections.toMap

      (typecheckerContext, aliasAnalysis.expressions.transformOrdered { (k, _) => unorderedTypecheckedSelectionsMap(k) })
    }

    val t_selectionTypechecking_joinTypechecking = System.nanoTime()

    // Ok, done with that noise.  Back to analyzing things.
    // `typecheckerContext` is the context for the expression as a
    // whole, but we need something less than that, as columns on
    // future join tables don't exist until the join actually occurs

    val typecheckedJoins = locally {
      val initialTypecheckerContext =
        Typechecker.Ctx(baseContext.implicitSchema, baseContext.visible.mapValues { tableRef =>
                          tableRef match {
                            case ref@TableRef.Primary =>
                              contextToSimpleSelection(baseContext.universe(baseContext.primaryDataset), ref)
                            case ref@TableRef.JoinPrimary(tn, _) =>
                              contextToSimpleSelection(baseContext.universe(tn), ref)
                            case ref@TableRef.Join(_) =>
                              // this shouldn't happen; the whole  point of starting from the base
                              // context is that a join _is't_ visible until after we've started
                              // typechecking its join condition.
                              throw UnexpectedlyVisibleJoin(ref)
                            case ref@TableRef.PreviousChainStep(_, _) =>
                              baseContext.implicitSchema.transformOrdered { (columnName, expr) =>
                                typed.ColumnRef(Qualified(ref, columnName), expr.typ)(expr.position)
                              }
                          }
                        })
      partiallyAnalyzedJoins.mapAccum(initialTypecheckerContext) { (typecheckerCtx, paJoin) =>
        val (analysis, augmentedTypecheckerCtx) =
          paJoin.originalJoin.from match {
            case JoinTable(from, alias, outputTableIdentifier) =>
              val a = JoinTableAnalysis[Qualified[ColumnName], Type](from.resourceName, outputTableIdentifier)
              val ctx =
                typecheckerCtx.copy(schemas = typecheckerCtx.schemas + (alias.getOrElse(from).resourceName -> contextToSimpleSelection(fullContext.universe(from.resourceName), a.outputTable)))

              (a, ctx)
            case JoinSelect(from, _, _, alias, outputTableIdentifier) =>
              val a = JoinSelectAnalysis[Qualified[ColumnName], Type](from.resourceName, outputTableIdentifier, paJoin.analysis)
              val ctx =
                typecheckerCtx.copy(schemas = typecheckerCtx.schemas + (alias.resourceName -> analysisToSimpleSelection(paJoin.analysis.last, a.outputTable)))

              (a, ctx)
          }
        log.debug("Typechecking join ON, current schemas: {}", augmentedTypecheckerCtx.schemas.keySet)
        val onExpr = typechecker(paJoin.originalJoin.on, augmentedTypecheckerCtx)
        (augmentedTypecheckerCtx, typed.Join[Qualified[ColumnName], Type](paJoin.originalJoin.typ, analysis, onExpr))
      }._2 // TODO: should we sanity check that _1 == typecheckerContext?
    }
    val typecheck = typechecker(_ : Expression, typecheckerContext)

    val t_joinTypechecking_whereTypechecking = System.nanoTime()
    val typecheckedWhere = query.where.map(typecheck)
    val t_whereTypechecking_groupByTypechecking = System.nanoTime()
    val typecheckedGroupBy = query.groupBys.map(typecheck)
    val t_groupByTypechecking_havingTypechecking = System.nanoTime()
    val typecheckedHaving = query.having.map(typecheck)
    val t_havingTypechecking_orderByTypechecking = System.nanoTime()
    val typecheckedOrderBy = query.orderBys.map { ob => ob -> typecheck(ob.expression) }
    val t_orderByTypechecking_isGrouped = System.nanoTime()
    val isGrouped = aggregateChecker(
      typecheckedSelections.values.toList,
      typecheckedWhere,
      typecheckedGroupBy,
      typecheckedHaving,
      typecheckedOrderBy.map(_._2))
    val t_isGrouped_validateTypes = System.nanoTime()

    def check(items: TraversableOnce[Expr], pred: Type => Boolean, onError: (TypeName, Position) => Throwable) {
      for(item <- items if !pred(item.typ)) throw onError(typeInfo.typeNameFor(item.typ), item.position)
    }

    check(typecheckedWhere, typeInfo.isBoolean, NonBooleanWhere)
    check(typecheckedGroupBy, typeInfo.isGroupable, NonGroupableGroupBy)
    check(typecheckedHaving, typeInfo.isBoolean, NonBooleanHaving)
    check(typecheckedOrderBy.map(_._2), typeInfo.isOrdered, UnorderableOrderBy)

    val t_validateTypes_end = System.nanoTime()

    if(log.isTraceEnabled) {
      log.trace("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
      log.trace("join partial analysis took {}ms", ns2ms(t_partialAnalysis_fullContext - t_start_partialAnalysis))
      log.trace("computing the full context took {}ms", ns2ms(t_fullContext_aliasAnalysis - t_partialAnalysis_fullContext))
      log.trace("alias analysis took {}ms", ns2ms(t_aliasAnalysis_selectionTypechecking - t_fullContext_aliasAnalysis))
      log.trace("typechecking selection took {}ms", ns2ms(t_selectionTypechecking_joinTypechecking - t_aliasAnalysis_selectionTypechecking))
      log.trace("typechecking joins took {}ms", ns2ms(t_joinTypechecking_whereTypechecking - t_selectionTypechecking_joinTypechecking))
      log.trace("typechecking WHERE took {}ms", ns2ms(t_whereTypechecking_groupByTypechecking - t_joinTypechecking_whereTypechecking))
      log.trace("typechecking GROUP BY took {}ms", ns2ms(t_groupByTypechecking_havingTypechecking - t_whereTypechecking_groupByTypechecking))
      log.trace("typechecking HAVING took {}ms", ns2ms(t_havingTypechecking_orderByTypechecking - t_groupByTypechecking_havingTypechecking))
      log.trace("typechecking ORDER BY took {}ms", ns2ms(t_orderByTypechecking_isGrouped - t_havingTypechecking_orderByTypechecking))
      log.trace("checking for aggregation took {}ms", ns2ms(t_isGrouped_validateTypes - t_orderByTypechecking_isGrouped))
      log.trace("validating the types of where/group/having/order clauses took {}ms", ns2ms(t_validateTypes_end - t_isGrouped_validateTypes))
      log.trace("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    }

    SoQLAnalysis(
      baseContext.implicitTableRef,
      isGrouped,
      query.distinct,
      typecheckedSelections,
      typecheckedJoins,
      typecheckedWhere,
      typecheckedGroupBy,
      typecheckedHaving,
      typecheckedOrderBy.map { case (ob, e) => typed.OrderBy(e, ob.ascending, ob.nullLast) },
      query.limit,
      query.offset,
      query.search)
  }
}

/**
  * The typed version of [[com.socrata.soql.ast.JoinSelect]]
  *
  * All joins must select from another table. A join may also join on sub-analysis. A join on a sub-analysis requires an
  * alias, but the alias is optional if the join is on a table-name only (e.g. "join 4x4").
  *
  *   "join 4x4" => JoinAnalysis(TableName(4x4, None), None) { aliasOpt = None }
  *   "join 4x4 as a" => JoinAnalysis(TableName(4x4, a), None) { aliasOpt = a }
  *   "join (select id from 4x4) as a" =>
  *     JoinAnalysis(TableName(4x4, None), Some(SubAnalysis(List(_select_id_), a))) { aliasOpt = a }
  *   "join (select c.id from 4x4 as c) as a" =>
  *     JoinAnalysis(TableName(4x4, Some(c)), Some(SubAnalysis(List(_select_id_), a))) { aliasOpt = a }
  */
sealed trait JoinAnalysis[ColumnId, Type] {
  val joinNum: Int
  val fromTable: TableRef.JoinPrimary
  val outputTable: TableRef

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId): JoinAnalysis[NewColumnId, Type]
  def mapAccumColumnIds[State, NewColumnId](s: State)(f: (State, ColumnId) => (State, NewColumnId)): (State, JoinAnalysis[NewColumnId, Type])
}

case class JoinSelectAnalysis[ColumnId, Type](fromTableName: ResourceName, joinNum: Int, analyses: NonEmptySeq[SoQLAnalysis[ColumnId, Type]]) extends JoinAnalysis[ColumnId, Type] {
  val fromTable = TableRef.JoinPrimary(fromTableName, joinNum)
  val outputTable = TableRef.Join(joinNum)

  SoQLAnalysis.assertSaneInputs(analyses, fromTable)

  override def toString: String = {
    val selectWithFromStr = analyses.head.toStringWithFrom(fromTable)
    val subAnasStr = (selectWithFromStr +: analyses.tail.map(_.toString)).mkString("(", " |> ", ")")
    s"$subAnasStr AS ${outputTable}"
  }

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) =
    JoinSelectAnalysis(fromTableName, joinNum, analyses.map(_.mapColumnIds(f)))

  def mapAccumColumnIds[State, NewColumnId](s: State)(f: (State, ColumnId) => (State, NewColumnId)) = {
    val (resultState, newAnalyses) = analyses.mapAccum(s) { (s1, a) => a.mapAccumColumnIds(s1)(f) }
    (resultState, JoinSelectAnalysis(fromTableName, joinNum, newAnalyses))
  }
}

case class JoinTableAnalysis[ColumnId, Type](fromTableName: ResourceName, joinNum: Int) extends JoinAnalysis[ColumnId, Type] {
  val fromTable = TableRef.JoinPrimary(fromTableName, joinNum)
  val outputTable = fromTable

  override def toString: String = {
    s"@${fromTable.resourceName.name} AS ${fromTable}"
  }

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) =
    JoinTableAnalysis(fromTableName, joinNum)

  def mapAccumColumnIds[State, NewColumnId](s: State)(f: (State, ColumnId) => (State, NewColumnId)) =
    (s, JoinTableAnalysis[NewColumnId, Type](fromTableName, joinNum))
}

/**
  * The typed version of [[com.socrata.soql.ast.Select]]
  *
  * Represents a single select statement, not including the from. Top-level soql selects have an implicit "from"
  * based on the current view. Joins do require a "from" (which is a member of the JoinAnalysis class).
  * A List[SoQLAnalysis] represents chained soql (e.g. "select a, b |> select a"), and is what is returned from a
  * top-level analysis of a soql string (see SoQLAnalyzer#analyzeFullQuery).
  *
  * the chained soql:
  *   "select id, a |> select a"
  * is equivalent to:
  *   "select a from (select id, a [from current view]) as alias"
  * and is represented as:
  *   List(analysis_id_a, analysis_id)
  *
  * @param isGrouped true iff there is a group by or aggregation function applied. Can be derived from the selection
  *                  and the groupBy
  */
case class SoQLAnalysis[ColumnId, Type](input: TableRef with TableRef.Implicit,
                                        isGrouped: Boolean,
                                        distinct: Boolean,
                                        selection: OrderedMap[ColumnName, typed.CoreExpr[ColumnId, Type]],
                                        joins: Seq[typed.Join[ColumnId, Type]],
                                        where: Option[typed.CoreExpr[ColumnId, Type]],
                                        groupBys: Seq[typed.CoreExpr[ColumnId, Type]],
                                        having: Option[typed.CoreExpr[ColumnId, Type]],
                                        orderBys: Seq[typed.OrderBy[ColumnId, Type]],
                                        limit: Option[BigInt],
                                        offset: Option[BigInt],
                                        search: Option[String]) {
  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId): SoQLAnalysis[NewColumnId, Type] =
    copy(
      selection = selection.mapValues(_.mapColumnIds(f)),
      joins = joins.map(_.mapColumnIds(f)),
      where = where.map(_.mapColumnIds(f)),
      groupBys = groupBys.map(_.mapColumnIds(f)),
      having = having.map(_.mapColumnIds(f)),
      orderBys = orderBys.map(_.mapColumnIds(f))
    )

  def mapAccumColumnIds[State, NewColumnId](s0: State)(f: (State, ColumnId) => (State, NewColumnId)): (State, SoQLAnalysis[NewColumnId, Type]) = {
    val (s1, newSel) = selection.mapAccumValues(s0) { (s, v) => v.mapAccumColumnIds(s)(f) }
    val (s2, newJoins) = joins.mapAccum(s1) { (s, v) => v.mapAccumColumnIds(s)(f) }
    val (s3, newWhere) = where.mapAccum(s2) { (s, v) => v.mapAccumColumnIds(s)(f) }
    val (s4, newGroupBys) = groupBys.mapAccum(s3) { (s, v) => v.mapAccumColumnIds(s)(f) }
    val (s5, newHaving) = having.mapAccum(s4) { (s, v) => v.mapAccumColumnIds(s)(f) }
    val (s6, newOrderBys) = orderBys.mapAccum(s5) { (s, v) => v.mapAccumColumnIds(s)(f) }
    (s6, copy(
       selection = newSel,
       joins = newJoins,
       where = newWhere,
       groupBys = newGroupBys,
       having = newHaving,
       orderBys = newOrderBys
     ))
  }

  def foldColumnIds[T](init: T)(f: (T, ColumnId) => T): T = {
    // This isn't particularly efficient, as it builds a copy of the
    // query, but queries aren't all _that_ big and this is obviously
    // correct.
    mapAccumColumnIds(init) { (s, cid) => (f(s, cid), cid) }._1
  }

  def foldTableRefs[A](seed: A)(f: (A, TableRef) => A): A =
    joins.foldLeft(seed) { (state, join) =>
      join.from match {
        case jta: JoinTableAnalysis[_, _] =>
          f(state, jta.fromTable)
        case jsa: JoinSelectAnalysis[_, _] =>
          jsa.analyses.iterator.foldLeft(f(f(state, join.from.fromTable), join.from.outputTable))(SoQLAnalysis.foldTableRefs(_, _)(f))
      }
    }

  private def toString(from: Option[TableRef]): String = {
    val distinctStr = if (distinct) "DISTINCT " else ""
    val selectStr = Some(s"SELECT $distinctStr$selection")
    val fromStr = from.map(t => s"FROM @$t")
    val joinsStr = itrToString(None, joins.map(_.toString), " ")
    val whereStr = itrToString("WHERE", where)
    val groupByStr = itrToString("GROUP BY", groupBys, ", ")
    val havingStr = itrToString("HAVING", having)
    val obStr = itrToString("ORDER BY", orderBys, ", ")
    val limitStr = itrToString("LIMIT", limit)
    val offsetStr = itrToString("OFFSET", offset)
    val searchStr = itrToString("SEARCH", search.map(Expression.escapeString))

    val parts = List(selectStr, fromStr, joinsStr, whereStr, groupByStr, havingStr, obStr, limitStr, offsetStr, searchStr)
    parts.flatString
  }

  def toStringWithFrom(fromTable: TableRef): String = toString(Some(fromTable))

  override def toString: String = toString(None)
}

object SoQLAnalysis {
  def allColumnRefs[ColumnId](analyses: NonEmptySeq[SoQLAnalysis[ColumnId, _]]): Set[ColumnId] =
    analyses.iterator.foldLeft(Set.empty[ColumnId]) { (set, analysis) => analysis.foldColumnIds(set)(_ + _) }

  def allTableRefs(analyses: NonEmptySeq[SoQLAnalysis[_, _]]): Set[TableRef] =
    foldTableRefs(Set.empty[TableRef], analyses)(_ + _)

  def joinChains[ColumnId, Type](analyses: NonEmptySeq[SoQLAnalysis[ColumnId, Type]]): Map[Int, typed.Join[ColumnId, Type]] =
    analyses.iterator.foldLeft(Map.empty[Int, typed.Join[ColumnId, Type]]) { (acc, analysis) =>
      analysis.joins.foldLeft(acc) { (acc, join) =>
        join.from match {
          case JoinTableAnalysis(_, i) => acc + (i -> join)
          case JoinSelectAnalysis(_, i, analyses) => acc ++ joinChains(analyses) + (i -> join)
        }
      }
    }

  private def foldTableRefs[A](seed: A, analyses: NonEmptySeq[SoQLAnalysis[_, _]])(f: (A, TableRef) => A): A =
    analyses.iterator.foldLeft(seed) { (s, a) =>
      a.foldTableRefs(s)(f)
    }

  private def foldTableRefs[A](seed: A, analysis: SoQLAnalysis[_, _])(f: (A, TableRef) => A): A =
    analysis.joins.foldLeft(seed) { (state, join) =>
      join.from match {
        case jta: JoinTableAnalysis[_, _] =>
          f(state, jta.fromTable)
        case jsa: JoinSelectAnalysis[_, _] =>
          jsa.analyses.iterator.foldLeft(f(f(state, join.from.fromTable), join.from.outputTable))(foldTableRefs(_, _)(f))
      }
    }

  def assertSaneInputs(analyses: Seq[SoQLAnalysis[_, _]], initial: TableRef with TableRef.PrimaryCandidate) {
    var expected: TableRef with TableRef.Implicit = initial
    for(analysis <- analyses) {
      assert(analysis.input == expected)
      expected = expected.next
    }
  }
  def assertSaneInputs(analyses: NonEmptySeq[SoQLAnalysis[_, _]], initial: TableRef with TableRef.PrimaryCandidate) {
    assertSaneInputs(analyses.seq, initial)
  }

  def merge[T](andFunction: MonomorphicFunction[T], stages: NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], T]]): NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], T]] =
    new Merger(andFunction).merge(stages)
}

private class Merger[T](andFunction: MonomorphicFunction[T]) {
  type Analysis = SoQLAnalysis[Qualified[ColumnName], T]
  type Expr = typed.CoreExpr[Qualified[ColumnName], T]

  def merge(stages: NonEmptySeq[Analysis]): NonEmptySeq[Analysis] = {
    val result =
      stages.foldLeft1(NonEmptySeq(_)) { case (acc, nextStage) =>
        tryMerge(acc.head , nextStage) match {
          case Some(merged) => acc.copy(head = merged)
          case None => acc.prepend(nextStage)
        }
      }.reverse.mapAccum(stages.head.input) { (input, analysis) =>
        (input.next, analysis.copy(input = input))
      }._2

    SoQLAnalysis.assertSaneInputs(result, stages.head.input.asInstanceOf[TableRef with TableRef.PrimaryCandidate])

    result
  }

  // Currently a search on the second query prevents merging, as its meaning ought to
  // be "search the output of the first query" rather than "search the underlying
  // dataset".  Unfortunately this means "select :*,*" isn't a left-identity of merge
  // for a query that contains a search.
  private def tryMerge(a: Analysis, b: Analysis): Option[Analysis] = (a, b) match {
    case (a, _) if (hasWindowFunction(a)) => None
    case (SoQLAnalysis(aInput, aIsGroup, false, aSelect, Nil, aWhere, aGroup, aHaving, aOrder, aLim, aOff, aSearch),
          SoQLAnalysis(_,      false,    false, bSelect, bJoins, None,   Nil,   None,    Nil,   bLim, bOff, None)) if
          // Do not merge when the previous soql is grouped and the next soql has joins
          // select g, count(x) as cx group by g |> select g, cx, @b.a join @b on @b.g=g
          // Newly introduced columns from joins cannot be merged and brought in w/o grouping and aggregate functions.
          !(aIsGroup && bJoins.nonEmpty) => // TODO: relaxed requirement on aFrom = bFrom? is this ok?
      // we can merge a change of only selection and limit + offset onto anything
      val (newLim, newOff) = Merger.combineLimits(aLim, aOff, bLim, bOff)
      Some(SoQLAnalysis(input = aInput,
                        isGrouped = aIsGroup,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = replaceJoinOnRefs(aSelect, bJoins),
                        where = aWhere,
                        groupBys = aGroup,
                        having = aHaving,
                        orderBys = aOrder,
                        limit = newLim,
                        offset = newOff,
                        search = aSearch))
    case (SoQLAnalysis(aInput, false, false, aSelect, Nil, aWhere, Nil, None, aOrder, None, None, aSearch),
          SoQLAnalysis(_,      false, false, bSelect, bJoins, bWhere, Nil, None, bOrder, bLim, bOff, None)) =>
      // Can merge a change of filter or order only if no window was specified on the left
      Some(SoQLAnalysis(input = aInput,
                        isGrouped = false,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = replaceJoinOnRefs(aSelect, bJoins),
                        where = mergeWhereLike(aSelect, aWhere, bWhere),
                        groupBys = Nil,
                        having = None,
                        orderBys = mergeOrderBy(aSelect, aOrder, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (SoQLAnalysis(aInput, false, false, aSelect, Nil, aWhere, Nil,     None,    _,      None, None, aSearch),
          SoQLAnalysis(_,      true, false, bSelect, bJoins, bWhere, bGroup, bHaving, bOrder, bLim, bOff, None)) =>
      // an aggregate on a non-aggregate
      Some(SoQLAnalysis(input = aInput,
                        isGrouped = true,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = replaceJoinOnRefs(aSelect, bJoins),
                        where = mergeWhereLike(aSelect, aWhere, bWhere),
                        groupBys = mergeGroupBy(aSelect, bGroup),
                        having = mergeWhereLike(aSelect, None, bHaving),
                        orderBys = mergeOrderBy(aSelect, Nil, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (SoQLAnalysis(aInput, true,  false, aSelect, Nil, aWhere, aGroup, aHaving, aOrder, None, None, aSearch),
          SoQLAnalysis(_,      false, false, bSelect, Nil, bWhere, Nil,      None,    bOrder, bLim, bOff, None)) =>
      // a non-aggregate on an aggregate -- merge the WHERE of the second with the HAVING of the first
      Some(SoQLAnalysis(input = aInput,
                        isGrouped = true,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        joins = Nil,
                        where = aWhere,
                        groupBys = aGroup,
                        having = mergeWhereLike(aSelect, aHaving, bWhere),
                        orderBys = mergeOrderBy(aSelect, aOrder, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (_, _) =>
      None
  }

  private def hasWindowFunction(a: Analysis): Boolean = {
    a.selection.exists {
      case (_, fc: com.socrata.soql.typed.FunctionCall[_, _]) =>
        fc.function.name == SpecialFunctions.WindowFunctionOver
      case _ =>
        false
    }
  }

  private def mergeSelection(a: OrderedMap[ColumnName, Expr],
                             b: OrderedMap[ColumnName, Expr]): OrderedMap[ColumnName, Expr] =
    // ok.  We don't need anything from A, but columnRefs in b's expr that refer to a's values need to get substituted
    b.mapValues(replaceRefs(a, _))

  private def mergeOrderBy(aliases: OrderedMap[ColumnName, Expr],
                           obA: Seq[typed.OrderBy[Qualified[ColumnName], T]],
                           obB: Seq[typed.OrderBy[Qualified[ColumnName], T]]): Seq[typed.OrderBy[Qualified[ColumnName], T]] =
    (obA, obB) match {
      case (Nil, Nil) => Seq.empty
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

  private def mergeGroupBy(aliases: OrderedMap[ColumnName, Expr], gb: Seq[Expr]): Seq[Expr] = {
    gb.map(replaceRefs(aliases, _))
  }

  private def replaceJoinOnRefs(a: OrderedMap[ColumnName, Expr],
                                bJoins: Seq[typed.Join[Qualified[ColumnName], T]]) =
    bJoins.map { bJoin =>
      bJoin match {
        case typed.InnerJoin(from, on) => typed.InnerJoin(from, replaceRefs(a, on))
        case typed.LeftOuterJoin(from, on) => typed.LeftOuterJoin(from, replaceRefs(a, on))
        case typed.RightOuterJoin(from, on) => typed.RightOuterJoin(from, replaceRefs(a, on))
        case typed.FullOuterJoin(from, on) => typed.FullOuterJoin(from, replaceRefs(a, on))
      }
    }

  private def replaceRefs(a: OrderedMap[ColumnName, Expr],
                          b: Expr): Expr =
    b match {
      case cr@typed.ColumnRef(Qualified(TableRef.PreviousChainStep(_, _), c), t) =>
        a.getOrElse(c, cr)
      case cr@typed.ColumnRef(_, _) =>
        cr
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
    require(aLim.fold(true)(_ >= 0), "Negative aLim")
    require(aOff.fold(true)(_ >= 0), "Negative aOff")
    require(bLim.fold(true)(_ >= 0), "Negative bLim")
    require(bOff.fold(true)(_ >= 0), "Negative bOff")
    (aLim, aOff, bLim, bOff) match {
      case (None, None, bLim, bOff) =>
        (bLim, bOff)
      case (None, Some(aOff), bLim, bOff) =>
        // first is unbounded to the right
        (bLim, Some(aOff + bOff.getOrElse(BigInt(0))))
      case (Some(aLim), aOff, Some(bLim), bOff) =>
        // both are bound to the right
        val trueAOff = aOff.getOrElse(BigInt(0))
        val trueBOff = bOff.getOrElse(BigInt(0)) + trueAOff
        val trueAEnd = trueAOff + aLim
        val trueBEnd = trueBOff + bLim

        val trueOff = trueBOff min trueAEnd
        val trueEnd = trueBEnd min trueAEnd
        val trueLim = trueEnd - trueOff

        (Some(trueLim), Some(trueOff))
      case (Some(aLim), aOff, None, bOff) =>
        // first is bound to the right but the second is not
        val trueAOff = aOff.getOrElse(BigInt(0))
        val trueBOff = bOff.getOrElse(BigInt(0)) + trueAOff

        val trueEnd = trueAOff + aLim
        val trueOff = trueBOff min trueEnd
        val trueLim = trueEnd - trueOff
        (Some(trueLim), Some(trueOff))
    }
  }
}
