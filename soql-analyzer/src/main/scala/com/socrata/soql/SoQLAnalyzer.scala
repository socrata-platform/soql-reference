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
import com.socrata.soql.collection._
import com.socrata.soql.typed.{ColumnRef, CoreExpr, Qualifier, TypedLiteral}
import Select._
import com.socrata.NonEmptySeq
import com.socrata.soql.mapping.ColumnIdMapper

/**
  * The type-checking version of [[com.socrata.soql.parsing.AbstractParser]]. Turns string soql statements into
  * Lists of Analysis objects (soql ast), while using type information about the tables to typecheck all
  * expressions.
  */
class SoQLAnalyzer[Type](typeInfo: TypeInfo[Type],
                         functionInfo: FunctionInfo[Type],
                         parserParameters: AbstractParser.Parameters = AbstractParser.defaultParameters)
{
  /** The typed version of [[com.socrata.soql.ast.Select]] */
  type Analysis = SoQLAnalysis[ColumnName, Type]
  type Expr = typed.CoreExpr[ColumnName, Type]
  type Qualifier = String
  type AnalysisContext = Map[Qualifier, DatasetContext[Type]]

  val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLAnalyzer[_]])

  def ns2ms(ns: Long) = ns / 1000000

  val aggregateChecker = new AggregateChecker[Type]

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
  def analyzeFullQuery(query: String)(implicit ctx: AnalysisContext): NonEmptySeq[Analysis] = {
    log.debug("Analyzing full query {}", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).selectStatement(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))
    analyze(parsed)
  }

  def analyzeFullQueryBinary(query: String)(implicit ctx: AnalysisContext): BinaryTree[Analysis] = {
    log.debug("Analyzing full query binarytree {}", query)
    val start = System.nanoTime()
    val parsed = new Parser(parserParameters).binaryTreeSelect(query)
    val end = System.nanoTime()
    log.trace("Parsing took {}ms", ns2ms(end - start))
    analyzeBinary(parsed)
  }

  def analyze(selects: NonEmptySeq[Select])(implicit ctx: AnalysisContext): NonEmptySeq[Analysis] = {
    selects.scanLeft1(analyzeWithSelection(_)(ctx))(analyzeInOuterSelectionContext(ctx))
  }

  def analyzeBinary(bSelect: BinaryTree[Select])(implicit ctx: AnalysisContext): BinaryTree[Analysis] = {
    bSelect match {
      case Leaf(s) =>
        Leaf(analyzeWithSelection(s)(ctx))
      case PipeQuery(ls, rs) =>
        val la = ls match {
          case Leaf(s) => Leaf(analyzeWithSelection(s)(ctx))
          case _ => analyzeBinary(ls)
        }
        val ra = rs match {
          case Leaf(s) =>
            val prev = la.outputSchema.leaf
            Leaf(analyzeInOuterSelectionContext(ctx)(prev, s))
          case _ =>
            analyzeBinary(ls)
        }
        PipeQuery(la, ra)
      case Compound(op, ls, rs) =>
        val la = analyzeBinary(ls)
        val ra = analyzeBinary(rs)
        validateTableShapes(la.outputSchema.leaf, ra.outputSchema.leaf, rs.outputSchema.leaf)
        Compound(op, la, ra)
    }
  }

  private def validateTableShapes(la: Analysis, ra: Analysis, rs: Select): Unit = {
    val left = la.selection.values
    val right = ra.selection.values
    if (left.size != right.size) {
      val pos = rs.selection.expressions.headOption.map(_.expression.position).getOrElse(NoPosition)
      throw NumberOfColumnsMismatch(left.size, right.size, pos)
    }
    left.zip(right).zipWithIndex.foreach {
      case ((l, r), idx) =>
        if (l.typ != r.typ) {
          val pos = rs.selection.expressions(idx).expression.position
          throw TypeOfColumnsMismatch(la.selection.keys.drop(idx).head.name + ":" + l.typ.toString,
                                      ra.selection.keys.drop(idx).head.name + ":" + r.typ.toString,
                                      pos)
        }
    }
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

  private def baseAnalysis(implicit ctx: AnalysisContext) = {

    val selections = ctx.map { case (qual, dsCtx) =>
      val q = if (qual == TableName.PrimaryTable.qualifier) None else Some(qual)
      dsCtx.schema.transform(typed.ColumnRef(q, _, _)(NoPosition))
    }
    // TODO: Enhance resolution of column name conflict from different tables in chained SoQLs
    val mergedSelection = selections.reduce(_ ++ _)
    SoQLAnalysis(isGrouped = false,
                 distinct = false,
                 selection = mergedSelection,
                 from = None,
                 joins = Nil,
                 where = None,
                 groupBys = Nil,
                 having = None,
                 orderBys = Nil,
                 limit = None,
                 offset = None,
                 search = None)
  }

  /** Turn framents of a SoQL SELECT statement into a typed `Analysis` object.  If no `selection` is provided,
    * one is generated based on whether the rest of the parameters indicate an aggregate query or not.  If it
    * is not an aggregate query, a selection list equivalent to `*` (i.e., every non-system column) is generated.
    * If it is an aggregate query, a selection list made up of the expressions from `groupBy` (if provided) together
    * with "`count(*)`" is generated.
    *
    * @param distinct   Reduce identical tuples into one.
    * @param selection  A selection list.
    * @param joins       A join list.
    * @param where      An expression to be used as the query's WHERE clause.
    * @param groupBys    A comma-separated list of expressions to be used as the query's GROUP BY cluase.
    * @param having     An expression to be used as the query's HAVING clause.
    * @param orderBys    A comma-separated list of expressions and sort-order specifiers to be uesd as the query's ORDER BY clause.
    * @param limit      A non-negative integer to be used as the query's LIMIT parameter
    * @param offset     A non-negative integer to be used as the query's OFFSET parameter
    * @param sourceFrom The analysis-chain of the query this query is based upon, if applicable.
    * @throws com.socrata.soql.exceptions.SoQLException if the query is syntactically or semantically erroneous.
    * @return The analysis of the query.  Note this should be appended to `sourceFrom` to form a full query-chain. */
  def analyzeSplitQuery(distinct: Boolean,
                        selection: Option[String],
                        joins: Option[String],
                        where: Option[String],
                        groupBys: Option[String],
                        having: Option[String],
                        orderBys: Option[String],
                        limit: Option[String],
                        offset: Option[String],
                        search: Option[String],
                        sourceFrom: Seq[Analysis] = Nil)(implicit ctx: AnalysisContext): Analysis = {
    log.debug("analyzing split query")

    val p = new Parser(parserParameters)

    val lastQuery =
      if(sourceFrom.isEmpty) baseAnalysis
      else sourceFrom.last

    def dispatch(distinct: Boolean, selection: Option[Selection],
                 joins: List[Join],
                 where: Option[Expression], groupBys: List[Expression], having: Option[Expression], orderBys: List[OrderBy], limit: Option[BigInt], offset: Option[BigInt], search: Option[String]) =
      selection match {
        case None => analyzeNoSelectionInOuterSelectionContext(lastQuery, distinct, joins, where, groupBys, having, orderBys, limit, offset, search)
        case Some(s) => analyzeInOuterSelectionContext(ctx)(lastQuery, Select(distinct, s, None, joins, where, groupBys, having, orderBys, limit, offset, search))
      }

    dispatch(
      distinct,
      selection.map(p.selection),
      joins.map(p.joins).toList.flatten,
      where.map(p.expression),
      groupBys.map(p.groupBys).toList.flatten,
      having.map(p.expression),
      orderBys.map(p.orderings).toList.flatten,
      limit.map(p.limit),
      offset.map(p.offset),
      search.map(p.search)
    )
  }

  def analyzeNoSelectionInOuterSelectionContext(lastQuery: Analysis,
                                                distinct: Boolean,
                                                joins: List[Join],
                                                where: Option[Expression],
                                                groupBys: List[Expression],
                                                having: Option[Expression],
                                                orderBys: List[OrderBy],
                                                limit: Option[BigInt],
                                                offset: Option[BigInt],
                                                search: Option[String]): Analysis = {
    implicit val fakeCtx = contextFromAnalysis(lastQuery)
    analyzeNoSelection(distinct, joins, where, groupBys, having, orderBys, limit, offset, search)
  }

  private def validateAlias(alias: String): Unit = {
    if(TableName.reservedNames(alias)) {
      // ugh, but the way joins are structured threading the position
      // of the alias through is tricky without major surgery.  Maybe
      // this can get better later.
      throw ReservedTableAlias(alias, NoPosition)
    }
  }

  private def joinCtx(joins: Seq[Join], parentFromContext: Map[Qualifier, DatasetContext[Type]])(implicit ctx: AnalysisContext): AnalysisContext = {
    joins.foldLeft(parentFromContext) { (acc, join) =>
      val jCtx = if (join.lateral) ctx ++ acc else ctx
      join.from match {
        case JoinQuery(selects, alias) =>
          validateAlias(alias)
          val analyses = analyzeBinary(selects)(jCtx)
          acc + contextFromAnalysis(alias, analyses.outputSchema.leaf)
        case JoinTable(tn@TableName(name, Some(alias))) =>
          validateAlias(alias)
          acc ++ aliasContext(tn, jCtx)
        case JoinTable(TableName(_, None)) =>
          acc
        case JoinFunc(_, _) =>
          throw UnexpectedJoinFunc()
      }
    }
  }

  // TODO: do we need this? wat
  def analyzeNoSelection(distinct: Boolean,
                         joins: List[Join],
                         where: Option[Expression],
                         groupBys: List[Expression],
                         having: Option[Expression],
                         orderBys: List[OrderBy],
                         limit: Option[BigInt],
                         offset: Option[BigInt],
                         search: Option[String])(implicit ctx: AnalysisContext): Analysis = {
    log.debug("No selection; doing typechecking of the other parts then deciding what to make the selection")

    val ctxFromJoins = joinCtx(joins, Map.empty)
    val ctxWithJoins = ctx ++ ctxFromJoins
    // ok, so the only tricky thing here is the selection itself.  Since it isn't provided, there are two cases:
    //   1. If there are groupBys, having, or aggregates in orderBy, then selection should be the equivalent of
    //      selecting the group-by clauses together with "count(*)".
    //   2. Otherwise, it should be the equivalent of selecting "*".
    val typechecker = new Typechecker(typeInfo, functionInfo)(ctxWithJoins)

    val typecheck = typechecker(_ : Expression, Map.empty, None)

    val t0 = System.nanoTime()
    val checkedWhere = where.map(typecheck)
    val t1 = System.nanoTime()
    val checkedGroupBy = groupBys.map(typecheck)
    val t2 = System.nanoTime()
    val checkedHaving = having.map(typecheck)
    val t3 = System.nanoTime()
    val checkedOrderBy = orderBys.map { ob => ob -> typecheck(ob.expression) }
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
      val count_* = FunctionCall(SpecialFunctions.StarFunc("count"), Nil, None)(NoPosition, NoPosition)
      val untypedSelectedExpressions = groupBys :+ count_*
      val names = AliasAnalysis(Selection(None, Seq.empty, untypedSelectedExpressions.map(SelectedExpression(_, None))), None).expressions.keys.toSeq
      val afterAliasAnalysis = System.nanoTime()

      log.trace("alias analysis took {}ms", ns2ms(afterAliasAnalysis - beforeAliasAnalysis))

      val typedSelectedExpressions = checkedGroupBy :+ typechecker(count_*, Map.empty, None)

      (names, typedSelectedExpressions)
    } else { // ok, no group by...
      log.debug("It is not grouped; selecting *")

      val beforeAliasAnalysis = System.nanoTime()
      val names = AliasAnalysis(Selection(None, Seq(StarSelection(None, Nil)), Nil), None).expressions.keys.toSeq
      val afterAliasAnalyis = System.nanoTime()

      log.trace("alias analysis took {}ms", ns2ms(afterAliasAnalyis - beforeAliasAnalysis))

      val typedSelectedExpressions = names.map { column =>
        typed.ColumnRef(None, column, ctx(TableName.PrimaryTable.qualifier).schema(column))(NoPosition)
      }

      (names, typedSelectedExpressions)
    }

    val checkedJoin = joins.map { j: Join =>
      val subAnalysisOpt = subAnalysis(j)(ctx)
      typed.Join(j.typ, JoinAnalysis(subAnalysisOpt), typecheck(j.on), j.lateral)
    }

    finishAnalysis(
      isGrouped,
      distinct,
      OrderedMap(names.zip(typedSelectedExpressions): _*),
      None,
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
    val nextCtx = prevCtx ++ initialCtx.view.filterKeys(qualifier => TableName.PrimaryTable.qualifier != qualifier)
    analyzeWithSelection(query)(nextCtx)
  }

  def contextFromAnalysis(a: Analysis): AnalysisContext = {
    val ctx = new DatasetContext[Type] {
      override val schema: OrderedMap[ColumnName, Type] = a.selection.withValuesMapped(_.typ)
    }
    Map(TableName.PrimaryTable.qualifier -> ctx)
  }

  private def contextFromAnalysis(qualifier: Qualifier, a: Analysis) = {
    val ctx = new DatasetContext[Type] {
      override val schema: OrderedMap[ColumnName, Type] = a.selection.withValuesMapped(_.typ)
    }
    (qualifier -> ctx)
  }

  def subAnalysis(join: Join)(ctx: AnalysisContext): Either[TableName, SubAnalysis[ColumnName,Type]] = {
    join.from match {
      case JoinQuery(selects, alias) =>
        val joinCtx: Map[Qualifier, DatasetContext[Type]] = ctx
        val analyses = analyzeBinary(selects)(joinCtx)
        Right(SubAnalysis(analyses, alias))
      case JoinTable(tn) =>
        Left(tn)
      case JoinFunc(_, _) =>
        throw UnexpectedJoinFunc()
    }
  }

  private def aliasContext(tableName: Option[TableName], ctx: AnalysisContext): AnalysisContext = {
    tableName match {
      case Some(TableName(TableName.SingleRow, alias)) =>
        alias.foreach(validateAlias(_))
        Map(alias.getOrElse(TableName.SingleRow) -> DatasetContext.empty[Type])
      case Some(TableName(name, Some(alias))) =>
        validateAlias(alias)
        val name1 = if (name == TableName.This) TableName.PrimaryTable.qualifier else name
        Map(alias -> ctx(name1))
      case Some(tn@TableName(name, None)) =>
        Map(TableName.PrimaryTable.qualifier -> ctx(tn.qualifier))
      case _ =>
        Map.empty
    }
  }

  private def aliasContext(from: TableName, ctx: AnalysisContext): AnalysisContext = {
    aliasContext(Some(from), ctx)
  }

  def analyzeWithSelection(query: Select)(implicit ctx: AnalysisContext): Analysis = {
    log.debug("There is a selection; typechecking all parts")
    val fromCtx = aliasContext(query.from, ctx)
    val ctxWithFrom = ctx ++ fromCtx
    val ctxFromJoins = ctxWithFrom ++ joinCtx(query.joins, fromCtx)
    val ctxWithJoins = ctx ++ ctxFromJoins
    val typechecker = new Typechecker(typeInfo, functionInfo)(ctxWithJoins)

    val t0 = System.nanoTime()
    val aliasAnalysis = AliasAnalysis(query.selection, query.from)(ctxWithJoins)
    val t1 = System.nanoTime()
    val typedAliases = aliasAnalysis.evaluationOrder.foldLeft(Map.empty[ColumnName, Expr]) { (acc, alias) =>
      acc + (alias -> typechecker(aliasAnalysis.expressions(alias), acc, query.from))
    }

    val typecheck = typechecker(_ : Expression, typedAliases, query.from)

    val (_, checkedJoin) = query.joins.foldLeft((Map.empty[Qualifier, DatasetContext[Type]], Seq.empty[typed.Join[ColumnName, Type]])) { (acc, j) =>
      val (jCtx, typedJoins) = acc
      val saCtx = if (j.lateral) ctxWithFrom ++ jCtx else ctx
      val subAnalysisOpt = subAnalysis(j)(saCtx)
      val accCtx = subAnalysisOpt match {
        case Right(SubAnalysis(analyses, alias)) =>
          jCtx + contextFromAnalysis(alias, analyses.outputSchema.leaf)
        case Left(tn@TableName(_, _)) =>
          jCtx ++ aliasContext(tn, saCtx)
      }
      val typedJoin = typed.Join(j.typ, JoinAnalysis(subAnalysisOpt), typecheck(j.on), j.lateral)
      (accCtx, typedJoins :+ typedJoin)
    }

    val outputs = OrderedMap(aliasAnalysis.expressions.keys.map { k => k -> typedAliases(k) }.toList : _*)
    val t2 = System.nanoTime()
    val checkedWhere = query.where.map(typecheck)
    val t3 = System.nanoTime()
    val checkedGroupBy = query.groupBys.map(typecheck)
    val t4 = System.nanoTime()
    val checkedHaving = query.having.map(typecheck)
    val t5 = System.nanoTime()
    val checkedOrderBy = query.orderBys.map { ob => ob -> typecheck(ob.expression) }
    val t6 = System.nanoTime()
    val isGrouped = aggregateChecker(
      outputs.values.toList,
      checkedWhere,
      checkedGroupBy,
      checkedHaving,
      checkedOrderBy.map(_._2))
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
                   outputs,
                   query.from,
                   checkedJoin, checkedWhere, checkedGroupBy, checkedHaving, checkedOrderBy,
                   query.limit, query.offset, query.search)
  }

  def finishAnalysis(isGrouped: Boolean,
                     distinct: Boolean,
                     output: OrderedMap[ColumnName, Expr],
                     from: Option[TableName],
                     joins: Seq[typed.Join[ColumnName, Type]],
                     where: Option[Expr],
                     groupBys: Seq[Expr],
                     having: Option[Expr],
                     orderBys: Seq[(OrderBy, Expr)],
                     limit: Option[BigInt],
                     offset: Option[BigInt],
                     search: Option[String]): Analysis =
  {
    def check(items: Iterable[Expr], pred: Type => Boolean, onError: (TypeName, Position) => Throwable): Unit = {
      for(item <- items if !pred(item.typ)) throw onError(typeInfo.typeNameFor(item.typ), item.position)
    }

    check(where, typeInfo.isBoolean, NonBooleanWhere)
    check(groupBys, typeInfo.isGroupable, NonGroupableGroupBy)
    check(having, typeInfo.isBoolean, NonBooleanHaving)
    check(orderBys.map(_._2), typeInfo.isOrdered, UnorderableOrderBy)

    SoQLAnalysis(
      isGrouped,
      distinct,
      output,
      from,
      joins,
      where,
      groupBys,
      having,
      orderBys.map { case (ob, e) => typed.OrderBy(e, ob.ascending, ob.nullLast) },
      limit,
      offset,
      search)
  }
}

/**
  * The typed version of [[com.socrata.soql.ast.SubSelect]]
  *
  * A SubAnalysis represents (potentially chained) soql that is required to have an alias
  * (because subqueries need aliases)
  */
case class SubAnalysis[ColumnId, Type](analyses: BinaryTree[SoQLAnalysis[ColumnId, Type]], alias: String)

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
case class JoinAnalysis[ColumnId, Type](subAnalysis: Either[TableName, SubAnalysis[ColumnId, Type]]) {

  val fromTables: Seq[TableName] = {
    subAnalysis match {
      case Left(tableName) => Seq(tableName)
      case Right(SubAnalysis(analyses, alias)) =>
        collectFromTables(analyses)
    }
  }

  private def collectFromTables(analysis: BinaryTree[SoQLAnalysis[ColumnId, Type]]): Seq[TableName] = {
    analysis match {
      case Compound(_p, l, r) =>
        collectFromTables(l) ++ collectFromTables(r)
      case x =>
        x.asLeaf.toSeq.flatMap(a => a.from.toSeq)
    }
  }

  val alias: Option[String] =  {
    subAnalysis match {
      case Left(TableName(_, alias)) => alias
      case Right(SubAnalysis(_, alias)) => Option(alias)
    }
  }

  def analyses: Option[BinaryTree[SoQLAnalysis[ColumnId, Type]]] = {
    subAnalysis match {
      case Left(TableName(_, alias)) => None
      case Right(SubAnalysis(analyses, _)) => Some(analyses)
    }
  }

  def mapColumnIds[NewColumnId](qColumnIdNewColumnIdMap: Map[(ColumnId, Qualifier), NewColumnId],
                                qColumnNameToQColumnId: (Qualifier, ColumnName) => (ColumnId, Qualifier),
                                columnNameToNewColumnId: ColumnName => NewColumnId,
                                columnIdToNewColumnId: ColumnId => NewColumnId): JoinAnalysis[NewColumnId, Type] = {

    val mappedSubAnalysis: Either[TableName, SubAnalysis[NewColumnId,Type]] = subAnalysis match {
      case Right(SubAnalysis(analyses, subAlias)) =>
        val mappedAnas = ColumnIdMapper.mapColumnIds(analyses)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        Right(SubAnalysis(mappedAnas, subAlias))
      case Left(tn) =>
        Left(tn)
    }

    JoinAnalysis(mappedSubAnalysis)
  }

  override def toString: String = {
    val (subAnasStr, aliasStrOpt) = subAnalysis match {
      case Right(SubAnalysis(subAnalysis, subAlias)) =>
        val selectStr = subAnalysis.toString
        (s"($selectStr)", Some(subAlias))
      case Left(TableName(name, alias)) =>
        (name, alias)
    }

    List(Some(subAnasStr), itrToString("AS", aliasStrOpt.map { a => "@" + TableName.removeValidPrefix(a)})).flatString
  }
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
case class SoQLAnalysis[ColumnId, Type](isGrouped: Boolean,
                                        distinct: Boolean,
                                        selection: OrderedMap[ColumnName, typed.CoreExpr[ColumnId, Type]],
                                        from: Option[TableName],
                                        joins: Seq[typed.Join[ColumnId, Type]],
                                        where: Option[typed.CoreExpr[ColumnId, Type]],
                                        groupBys: Seq[typed.CoreExpr[ColumnId, Type]],
                                        having: Option[typed.CoreExpr[ColumnId, Type]],
                                        orderBys: Seq[typed.OrderBy[ColumnId, Type]],
                                        limit: Option[BigInt],
                                        offset: Option[BigInt],
                                        search: Option[String]) {

  /**
   * This version of mapColumnsId is incomplete and for rollup/rewrite in soda fountain
   * where join, union recursive features of queries are not supported.
   * Normally, the version that takes 4 parameters should be used
   */
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): SoQLAnalysis[NewColumnId, Type] = {
    copy(
      selection = selection.withValuesMapped(_.mapColumnIds(f)),
      joins = joins.map(_.mapColumnIds(f)),
      where = where.map(_.mapColumnIds(f)),
      groupBys = groupBys.map(_.mapColumnIds(f)),
      having = having.map(_.mapColumnIds(f)),
      orderBys = orderBys.map(_.mapColumnIds(f))
    )
  }

  def mapColumnIds[NewColumnId](qColumnIdNewColumnIdMap: Map[(ColumnId, Qualifier), NewColumnId],
                                qColumnNameToQColumnId: (Qualifier, ColumnName) => (ColumnId, Qualifier),
                                columnNameToNewColumnId: ColumnName => NewColumnId,
                                columnIdToNewColumnId: ColumnId => NewColumnId): SoQLAnalysis[NewColumnId, Type] = {

    lazy val columnsByQualifier: Map[Qualifier, Map[(ColumnId, Qualifier), NewColumnId]] =
      qColumnIdNewColumnIdMap.groupBy(_._1._2) + (Some(TableName.SingleRow) -> Map.empty)

    val qColumnIdNewColumnIdMapWithFrom = this.from.foldLeft(qColumnIdNewColumnIdMap) { (acc, tableName) =>
      val qual = tableName match {
        case TableName(TableName.This, alias@Some(_)) =>
          alias
        case _ =>
          Some(tableName.name)
      }
      val schema = columnsByQualifier(qual)
      tableName match {
        case TableName(name, a@Some(_)) =>
          acc ++ schema.map { case ((columnId, _), newColumnId) => ((columnId, a), newColumnId)}
        case TableName(name, None) =>
          acc ++ schema.map { case ((columnId, _), newColumnId) => ((columnId, None), newColumnId)}
      }
    }

    def columnsFromJoin(j: typed.Join[ColumnId, Type]): Seq[((ColumnId, Qualifier), NewColumnId)] = {
      j.from.subAnalysis match {
        case Left(TableName(_, None)) =>
          Seq.empty // Nothing new added
        case Left(TableName(name, a@Some(alias))) =>
          // we don't previously handle this case - that means join simple table cannot be renamed
          // like in "join @aaaa-aaaa as a1" that as a1 will be ignored.  You still need to refer to columns in @aaaa-aaaa
          // without qualifier which should be wrong.  Hope noone is using that.
          val schema = columnsByQualifier(Some(name))
          schema.map { case ((columnId, _), newColumnId) =>
            ((columnId, a), newColumnId)
          }.toSeq
        case Right(SubAnalysis(ana, alias)) =>
          ana.outputSchema.leaf.selection.map { case (columnName, _) =>
            qColumnNameToQColumnId(j.from.alias, columnName) -> columnNameToNewColumnId(columnName)
          }.toSeq
      }
    }

    val newColumnsFromJoin = joins.flatMap(j => columnsFromJoin(j)).toMap

    val qColumnIdNewColumnIdWithJoinsMap = qColumnIdNewColumnIdMapWithFrom ++ newColumnsFromJoin

    val (_, mappedJoins) = joins.foldLeft((Map.empty[(ColumnId, Qualifier), NewColumnId], Seq.empty[typed.Join[NewColumnId, Type]])) { (acc, join) =>
      val (accQColumnIdNewColumnIdMap, accJoins) = acc
      val nextAccQColumnIdNewColumnIdMap = accQColumnIdNewColumnIdMap ++ columnsFromJoin(join)
      val joinQColumnIdNewColumnIdMap = if (join.lateral) qColumnIdNewColumnIdMap ++ nextAccQColumnIdNewColumnIdMap
                                        else qColumnIdNewColumnIdMap
      val mappedAnalysis = join.from.mapColumnIds(joinQColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
      val mappedOn = join.on.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))
      val mappedJoin = typed.Join(join.typ, mappedAnalysis, mappedOn, join.lateral)
      (nextAccQColumnIdNewColumnIdMap, accJoins :+ mappedJoin)
    }

    copy(
      selection = selection.withValuesMapped(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      joins = mappedJoins,
      where = where.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      groupBys = groupBys.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      having = having.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap))),
      orderBys = orderBys.map(_.mapColumnIds(Function.untupled(qColumnIdNewColumnIdWithJoinsMap)))
    )
  }

  private def toString(from: Option[TableName]): String = {
    val distinctStr = if (distinct) "DISTINCT " else ""
    val selectStr = Some(s"SELECT $distinctStr$selection")
    val fromStr = this.from.orElse(from).map(t => s"FROM $t")
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

  def toStringWithFrom(fromTable: TableName): String = toString(Some(fromTable))

  override def toString: String = toString(None)
}

object SoQLAnalysis {
  def merge[T](andFunction: MonomorphicFunction[T], stages: BinaryTree[SoQLAnalysis[ColumnName, T]]): BinaryTree[SoQLAnalysis[ColumnName, T]] =
    new Merger(andFunction).merge(stages)
}

private class Merger[T](andFunction: MonomorphicFunction[T]) {
  type Analysis = SoQLAnalysis[ColumnName, T]
  type Expr = typed.CoreExpr[ColumnName, T]

  def merge(stages: BinaryTree[Analysis]): BinaryTree[Analysis] = {
    stages match {
      case PipeQuery(l, r) =>
        val ml = merge(l)
        leftMergeCandidate(ml) match {
          case Some(mlCandidate) =>
            val ra = r.asLeaf.getOrElse(throw RightSideOfChainQueryMustBeLeaf(NoPosition))
            tryMerge(mlCandidate.leaf, ra) match {
              case Some(merged) =>
                if (ml.asLeaf.isDefined) Leaf(merged)
                else ml.replace(mlCandidate, Leaf(merged))
              case None =>
                PipeQuery(ml, r)
            }
          case None =>
            PipeQuery(ml, r)
        }
      case Compound(op, l, r) =>
        val nl = merge(l)
        val nr = merge(r)
        Compound(op, nl, nr)
      case Leaf(_) =>
        stages
    }
  }

  private def leftMergeCandidate(left: BinaryTree[Analysis]): Option[Leaf[Analysis]] = {
    left match {
      case leaf@Leaf(_) => Some(leaf)
      case PipeQuery(_, _) => Some(left.outputSchema)
      case Compound(_, _, _) => None // UNIONs etc are not mergeable
    }
  }

  // Currently a search on the second query prevents merging, as its meaning ought to
  // be "search the output of the first query" rather than "search the underlying
  // dataset".  Unfortunately this means "select :*,*" isn't a left-identity of merge
  // for a query that contains a search.
  private def tryMerge(a: Analysis, b: Analysis): Option[Analysis] = (a, b) match {
    case (a, _) if (hasWindowFunction(a)) => None
    case (SoQLAnalysis(aIsGroup, false, aSelect, aFrom, Nil, aWhere, aGroup, aHaving, aOrder, aLim, aOff, None),
          SoQLAnalysis(false,    false, bSelect, None, bJoins, None,   Nil,   None,    Nil,   bLim, bOff, None)) if
          // Do not merge when the previous soql is grouped and the next soql has joins
          // select g, count(x) as cx group by g |> select g, cx, @b.a join @b on @b.g=g
          // Newly introduced columns from joins cannot be merged and brought in w/o grouping and aggregate functions.
          !(aIsGroup && bJoins.nonEmpty) && // TODO: relaxed requirement on aFrom = bFrom? is this ok?
          !aGroup.exists(hasLiteral)
          =>
      // we can merge a change of only selection and limit + offset onto anything
      val (newLim, newOff) = Merger.combineLimits(aLim, aOff, bLim, bOff)
      Some(SoQLAnalysis(isGrouped = aIsGroup,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        from = aFrom,
                        joins = bJoins.map(join => join.copy(on = replaceRefs(aSelect, join.on), lateral = join.lateral)),
                        where = aWhere,
                        groupBys = aGroup,
                        having = aHaving,
                        orderBys = aOrder,
                        limit = newLim,
                        offset = newOff,
                        search = None))
    case (SoQLAnalysis(false, false, aSelect, aFrom, Nil, aWhere, Nil, None, aOrder, None, None, aSearch),
          SoQLAnalysis(false, false, bSelect, None, bJoins, bWhere, Nil, None, bOrder, bLim, bOff, None)) =>
      // Can merge a change of filter or order only if no window was specified on the left
      Some(SoQLAnalysis(isGrouped = false,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        from = aFrom,
                        joins = bJoins.map(join => join.copy(on = replaceRefs(aSelect, join.on), lateral = join.lateral)),
                        where = mergeWhereLike(aSelect, aWhere, bWhere),
                        groupBys = Nil,
                        having = None,
                        orderBys = mergeOrderBy(aSelect, aOrder, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = aSearch))
    case (SoQLAnalysis(false, false, aSelect, aFrom, Nil, aWhere, Nil,     None,    _,      None, None, None),
          SoQLAnalysis(true, false, bSelect, None, bJoins, bWhere, bGroup, bHaving, bOrder, bLim, bOff, None)) =>
      // an aggregate on a non-aggregate
      Some(SoQLAnalysis(isGrouped = true,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        from = aFrom,
                        joins = bJoins.map(join => join.copy(on = replaceRefs(aSelect, join.on), lateral = join.lateral)),
                        where = mergeWhereLike(aSelect, aWhere, bWhere),
                        groupBys = mergeGroupBy(aSelect, bGroup),
                        having = mergeWhereLike(aSelect, None, bHaving),
                        orderBys = mergeOrderBy(aSelect, Nil, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = None))
    case (SoQLAnalysis(true,  false, aSelect, aFrom, Nil, aWhere, aGroup, aHaving, aOrder, None, None, None),
          SoQLAnalysis(false, false, bSelect, None, Nil, bWhere, Nil,      None,    bOrder, bLim, bOff, None))
          if !aGroup.exists(hasLiteral)
          =>
      // a non-aggregate on an aggregate -- merge the WHERE of the second with the HAVING of the first
      Some(SoQLAnalysis(isGrouped = true,
                        distinct = false,
                        selection = mergeSelection(aSelect, bSelect),
                        from = aFrom,
                        joins = Nil,
                        where = aWhere,
                        groupBys = aGroup,
                        having = mergeWhereLike(aSelect, aHaving, bWhere),
                        orderBys = mergeOrderBy(aSelect, aOrder, bOrder),
                        limit = bLim,
                        offset = bOff,
                        search = None))
    case (_, _) =>
      None
  }

  private def hasWindowFunction(a: Analysis): Boolean = {
    a.selection.exists {
      case (_, expr) =>
        hasWindowFunction(expr)
      case _ =>
        false
    }
  }

  private def hasWindowFunction(e: CoreExpr[_, _]): Boolean = {
    e match {
      case fc: com.socrata.soql.typed.FunctionCall[_, _] =>
        fc.window.nonEmpty || fc.parameters.exists(hasWindowFunction)
      case _ =>
        false
    }
  }

  private def hasLiteral(e: CoreExpr[_, _]): Boolean = {
    e match {
      case fc: com.socrata.soql.typed.FunctionCall[_, _] =>
        fc.parameters.exists(hasLiteral)
      case _: TypedLiteral[_] =>
        true
      case _ =>
        false
    }
  }

  private def mergeSelection(a: OrderedMap[ColumnName, Expr],
                             b: OrderedMap[ColumnName, Expr]): OrderedMap[ColumnName, Expr] =
    // ok.  We don't need anything from A, but columnRefs in b's expr that refer to a's values need to get substituted
    b.withValuesMapped(replaceRefs(a, _))

  private def mergeOrderBy(aliases: OrderedMap[ColumnName, Expr],
                           obA: Seq[typed.OrderBy[ColumnName, T]],
                           obB: Seq[typed.OrderBy[ColumnName, T]]): Seq[typed.OrderBy[ColumnName, T]] =
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
      case (Some(a), Some(b)) => Some(typed.FunctionCall(andFunction, List(a, replaceRefs(aliases, b)), None, None)(NoPosition, NoPosition))
    }

  private def mergeGroupBy(aliases: OrderedMap[ColumnName, Expr], gb: Seq[Expr]): Seq[Expr] = {
    gb.map(replaceRefs(aliases, _))
  }

  private def replaceRefs(a: OrderedMap[ColumnName, Expr],
                             b: Expr): Expr =
    b match {
      case cr@typed.ColumnRef(Some(q), c, t) =>
        cr
      case cr@typed.ColumnRef(None, c, t) =>
        a.getOrElse(c, cr)
      case tl: typed.TypedLiteral[T] =>
        tl
      case fc@typed.FunctionCall(f, params, filter, window) =>
        val fi = filter.map(x => replaceRefs(a, x))
        val w = window.map {
          case typed.WindowFunctionInfo(partitions, orderings, frames) =>
            typed.WindowFunctionInfo(partitions.map(replaceRefs(a, _)),
                                     orderings.map(ob => ob.copy(expression = replaceRefs(a, ob.expression))),
                                     frames)
        }
        typed.FunctionCall(f, params.map(replaceRefs(a, _)), fi, w)(fc.position, fc.functionNamePosition)
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
