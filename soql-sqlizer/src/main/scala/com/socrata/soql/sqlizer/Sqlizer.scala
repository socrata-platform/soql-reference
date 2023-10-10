package com.socrata.soql.sqlizer

import scala.collection.compat._
import scala.collection.mutable.Stack
import scala.util.parsing.input.{Position, NoPosition}
import scala.reflect.ClassTag

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, Provenance}
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.{SimpleDocStream, SimpleDocTree, tree}

// Ok, so the main source of trickiness here is this:
//
// Compound columns are stored with each subcolumn mapped to a
// separate physical column.  We want to keep it this way as long as
// possible, but most functions (see below for a list of exceptions)
// want to operate on them as single values.  So: during sqlization,
// each schema can represent columns as "expanded" or "compressed".
// Expanded columns can be converted to compressed columns by sql that
// looks like this:
//
//   nullify_record(ROW(subcol1, subcol2, ...) :: some_specific_type)
//
// nullify_record is defined like this:
//
// CREATE OR REPLACE FUNCTION nullify_record(x ANYELEMENT) RETURNS ANYELEMENT AS $$
//   VALUES (CASE WHEN x IS NULL THEN NULL ELSE x END);
// $$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
//
// and exists beause of a weird (postgres-specific?) inconsistency
// where `row(null, null) is null` is true (which we want) but
// `coalesce` treats it as not-null (which we don't).
//
// As a result, when a (sub)query is sqlized, it will return
// information about its schema which includes whether a compound
// column has gotten compressed or not.
//
// The functions that can act on an expanded compound column are:
//   * subcolumn selectors (trivial: just sqlize as the selected subcolumn)
//   * IS NULL (trivial: just AND together IS NULL on the subcolumns)
//   * IS NOT NULL (trivial: just or OR together IS NOT NULL on the subcolumns)
//
// More trickiness that falls out of this: interactions with GROUP BY.
// The GROUP BY expressions must be a subset of the post-GROUP BY
// (i.e., HAVING, ORDER BY, DISTINCT ON, and SELECT list expressions)
// so these are the good cases:
//
//    * the columns are EXPANDED in the GROUP BY and EITHER COMPRESSED
//      OR EXPANDED later (because being expanded in the group-by means
//      that later passing the columns to the non-aggregate compressor
//      function is fine)
//    * the columns are COMPRESSED in the GROUP BY and COMPRESSED later
//
// Note that since a column, once compressed, remains compressed, and
// the compressedness of a column only depends on the input schemas
// and what it itself contains, this _should_ just fall naturally out
// of expression sqlization, but I'm mentioning it here for
// completeness.
//
// Final bit of trickiness: select list references.  SLRs _change_
// depending on whether compound columns are expanded or compressed in
// the select list.  Specifically, expanded columns affect the indices
// of subsequent selected columns, and the sqlization of a select list
// reference depends on whether the output column is compressed or
// not.
//
// Fortunately, as long as we sqlize our select list first, this is a
// relatively straightforward transformation.  HAVING expressions do
// not use select list references, and GROUP BY / ORDER BY / DISTINCT
// ON select list references can just expand into lists of select-list
// references (in the order by case, distributing asc/desc nulls
// first/last over the list)

class Sqlizer[MT <: MetaTypes with MetaTypesExt](
  val funcallSqlizer: FuncallSqlizer[MT],
  val exprSqlFactory: ExprSqlFactory[MT],
  val namespace: SqlNamespaces[MT],
  val rewriteSearch: RewriteSearch[MT],
  val toProvenance: types.ToProvenance[MT],
  val isRollup: types.DatabaseTableName[MT] => Boolean,
  val mkRepProvider: (Sqlizer[MT], Map[AutoTableLabel, types.DatabaseTableName[MT]], MT#ExtraContext) => Rep.Provider[MT]
) extends SqlizerUniverse[MT] {
  type DynamicContext = Sqlizer.DynamicContext[MT]

  val exprSqlizer = new ExprSqlizer(funcallSqlizer, exprSqlFactory)

  def apply(
    analysis: SoQLAnalysis[MT],
    ec: ExtraContext
  )(implicit ct: ClassTag[CV]): Sqlizer.Result[MT] = {
    val (sql, augmentedSchema, dynamicContext) = sqlize(analysis, true, ec)
    Sqlizer.Result(
      sql,
      new ResultExtractor(augmentedSchema),
      ec.finish()
    )
  }

  // This produces a SQL statement, but doesn't necessarily apply any
  // postprocessing transforms to the final output columns (which you
  // wouldn't want if, for example, you were converting a Statement
  // into a rollup table).
  def sqlize(
    analysis: SoQLAnalysis[MT],
    rewriteOutputColumns: Boolean,
    ec: ExtraContext
  ): (Doc, AugmentedSchema, DynamicContext) = {
    val rewritten = rewriteSearch(analysis.statement)
    val repFor = mkRepProvider(this, analysis.physicalTableMap, ec)
    val dynamicContext = Sqlizer.DynamicContext[MT](
      repFor,
      ProvenanceTracker(rewritten, e => repFor(e.typ).provenanceOf(e), toProvenance, isRollup),
      analysis.physicalTableMap,
      new GensymProvider(namespace),
      ec
    )

    val (sql, augmentedSchema) = sqlizeStatement(rewritten, Map.empty, dynamicContext, rewriteOutputColumns)

    (sql, augmentedSchema, dynamicContext)
  }

  private def sqlizeStatement(
    stmt: Statement,
    availableSchemas: AvailableSchemas,
    dynamicContext: DynamicContext,
    topLevel: Boolean
  ): (Doc, AugmentedSchema) = {
    stmt match {
      case select: Select => sqlizeSelect(select, availableSchemas, dynamicContext, topLevel)
      case values: Values => sqlizeValues(values, availableSchemas, dynamicContext, topLevel)
      case combinedTables: CombinedTables => sqlizeCombinedTables(combinedTables, availableSchemas, dynamicContext, topLevel)
      case cte: CTE => ???
    }
  }

  private def sqlizeCombinedTables(
    combinedTables: CombinedTables,
    availableSchemas: AvailableSchemas,
    dynamicContext: DynamicContext,
    topLevel: Boolean
  ): (Doc, AugmentedSchema) = {
    // Ok, this is a little complicated because both the left and
    // right need to have the same database schema, so we need to line
    // up the compressedness of both sides.  Fortunately combining
    // tables destroys ordering operations, so we can introduce a
    // layer to cause that to happen if it's necessary.

    val CombinedTables(op, leftStmt, rightStmt) = combinedTables

    val (leftDoc, leftSchema) = sqlizeStatement(leftStmt, availableSchemas, dynamicContext, topLevel)
    val (rightDoc, rightSchema) = sqlizeStatement(rightStmt, availableSchemas, dynamicContext, topLevel)

    assert(leftSchema.values.map(_.typ) == rightSchema.values.map(_.typ))

    val (effectiveLeftDoc, effectiveRightDoc, effectiveSchema) = {
        // ok, we know the types line up, so we just need to do the
        // compressedness

        sealed abstract class NeedCompress
        case class CompressLeft(label: ColumnLabel) extends NeedCompress
        case class CompressRight(label: ColumnLabel) extends NeedCompress
        case object NoChange extends NeedCompress

        val needCompress = leftSchema.iterator.zip(rightSchema.iterator).map { case (l, r) =>
          val (leftLabel, leftAugTyp) = l
          val (rightLabel, rightAugTyp) = r
          (leftAugTyp.isExpanded, rightAugTyp.isExpanded) match {
            case (false, true) => CompressRight(rightLabel)
            case (true, false) => CompressLeft(leftLabel)
            case _ => NoChange
          }
        }.toVector

        val (recompressedLeftDoc, recompressedLeftSchema) =
          if(needCompress.exists(_.isInstanceOf[CompressLeft])) {
            val mask = needCompress.collect { case CompressLeft(lbl) => lbl }.toSet
            recompress(dynamicContext, leftDoc, leftSchema, mask)
          } else {
            (leftDoc, leftSchema)
          }

        val (recompressedRightDoc, recompressedRightSchema) =
          if(needCompress.exists(_.isInstanceOf[CompressRight])) {
            val mask = needCompress.collect { case CompressRight(lbl) => lbl }.toSet
            recompress(dynamicContext, rightDoc, rightSchema, mask)
          } else {
            (rightDoc, rightSchema)
          }

        assert(recompressedLeftSchema.values.toSeq == recompressedRightSchema.values.toSeq)

        (recompressedLeftDoc, recompressedRightDoc, recompressedLeftSchema)
      }

    (effectiveLeftDoc.parenthesized +#+ sqlizeOp(op) +#+ effectiveRightDoc.parenthesized, effectiveSchema)
  }

  private def recompress(dynamicContext: DynamicContext, stmt: Doc, schema: AugmentedSchema, mask: Set[ColumnLabel]): (Doc, AugmentedSchema) = {
    val tmpTable = Doc(dynamicContext.gensymProvider.next())
    val schemaOrder = schema.keys.toSeq

    val selectionDoc = Seq(
      Some(d"SELECT"),
      Some(
        schemaOrder.flatMap { columnLabel =>
          val typ = schema(columnLabel).typ
          val rep = dynamicContext.repFor(typ)

          if(mask(columnLabel)) {
            val underlyingColumnSqls = rep.expandedDatabaseColumns(columnLabel).map { dbColName =>
              tmpTable ++ d"." ++ Doc(dbColName)
            }
            Seq(exprSqlFactory.compress(None, underlyingColumnSqls) +#+ d"AS" +#+ Doc(rep.compressedDatabaseColumn(columnLabel)))
          } else if(schema(columnLabel).isExpanded) {
            rep.expandedDatabaseColumns(columnLabel).map { dbColName =>
              tmpTable ++ d"." ++ Doc(dbColName) +#+ d"AS" +#+ Doc(dbColName)
            }
          } else {
            Seq(tmpTable ++ d"." ++ Doc(rep.compressedDatabaseColumn(columnLabel)) +#+ d"AS" +#+ Doc(rep.compressedDatabaseColumn(columnLabel)))
          }
        }.commaSep
      ).filter { _ => schemaOrder.nonEmpty }
    ).flatten.vsep.nest(2)
    val fromDoc = (d"FROM" ++ Doc.lineSep ++ stmt.parenthesized +#+ d"AS" +#+ tmpTable).nest(2)

    val doc = Seq(
      selectionDoc,
      fromDoc
    ).vsep.group

    val newSchema = OrderedMap() ++ schema.map { case (lbl, augTyp) =>
      if(mask(lbl)) lbl -> augTyp.copy(isExpanded = false)
      else lbl -> augTyp
    }

    (doc, newSchema)
  }

  private def sqlizeOp(op: TableFunc): Doc =
    op match {
      case TableFunc.Union => d"UNION"
      case TableFunc.UnionAll => d"UNION ALL"
      case TableFunc.Intersect => d"INTERSECT"
      case TableFunc.IntersectAll => d"INTERSECT ALL"
      case TableFunc.Minus => d"MINUS"
      case TableFunc.MinusAll => d"MINUS ALL"
    }

  private def sqlizeValues(
    values: Values,
    availableSchemas: AvailableSchemas,
    dynamicContext: DynamicContext,
    topLevel: Boolean
  ): (Doc, AugmentedSchema) = {
    val exprSqlizer = this.exprSqlizer.withContext(availableSchemas, Vector.empty, dynamicContext)

    // Ugh, why are valueses so weird?
    // Ok so, if we have a single row, just sqlize it as a FROMless SELECT.
    if(values.values.tail.isEmpty) {
      val row = values.values.head.iterator.map(exprSqlizer.sqlize).toVector

      val physicalSchema: Seq[(Doc, ColumnName)] = values.labels.toSeq.lazyZip(row).lazyZip(values.schema.values).flatMap { (label, expr, schemaEntry) =>
        val rep = dynamicContext.repFor(expr.typ)
        expr match {
          case _ : ExprSql.Compressed[MT] =>
            Seq(rep.compressedDatabaseColumn(label) -> schemaEntry.name)
          case _ : ExprSql.Expanded[MT] =>
            rep.expandedDatabaseColumns(label).map(_ -> schemaEntry.name)
        }
      }

      val augmentedSchema: OrderedMap[ColumnLabel, AugmentedType] =
        OrderedMap() ++ values.labels.lazyZip(row).map { case (label, expr) =>
          val isExpanded = expr match {
            case _ : ExprSql.Compressed[_] => false
            case _ : ExprSql.Expanded[_] => true
          }
          label -> AugmentedType(dynamicContext.repFor(expr.typ), isExpanded)
        }

      val names = physicalSchema.iterator.map { case (label, name) =>
        Doc(label).annotate(SqlizeAnnotation.OutputName[MT](name))
      }
      val selectList = row.flatMap { expr =>
        expr match {
          case cmp: ExprSql.Compressed[MT] =>
            Seq(cmp.sql +#+ d"AS" +#+ names.next())
          case exp: ExprSql.Expanded[MT] =>
            exp.sqls.map { sql => sql +#+ d"AS" +#+ names.next() }
        }
      }.commaSep
      assert(!names.hasNext)

      (d"SELECT" +#+ selectList, augmentedSchema)
    } else {
      // We have more than one row, so because valueses don't have
      // portable names, so we'll need to do a weird thing.
      // Specifically we'll be generating
      //   select t.col1 as col1, ... from (values rows...) as t(col1, ...)
      // In addition, a wrinkle!
      // The rows must all have the same physical schema
      // so if a cell is compressed in one row, it must be compressed in _all_ rows
      val rawRowExprs = values.values.iterator.map { row =>
        row.iterator.map(exprSqlizer.sqlize).toVector
      }.toVector

      val compressedColumnIndices = rawRowExprs.foldLeft(Set.empty[Int]) { (acc, row) =>
        row.iterator.zipWithIndex.foldLeft(acc) { (acc, cellIdx) =>
          val (cell, idx) = cellIdx
          cell match {
            case _ : ExprSql.Compressed[MT] => acc + idx
            case _ : ExprSql.Expanded[MT] => acc
          }
        }
      }

      val rowExprs = rawRowExprs.map { rawRow =>
        rawRow.iterator.zipWithIndex.map { case (cell, idx) =>
          if(compressedColumnIndices(idx)) cell.compressed
          else cell
        }.toVector
      }

      val physicalSchema: Seq[(Doc, ColumnName)] = values.schema.iterator.zipWithIndex.flatMap { case ((label, nameEntry), idx) =>
        if(compressedColumnIndices(idx)) Seq(dynamicContext.repFor(nameEntry.typ).compressedDatabaseColumn(label) -> nameEntry.name)
        else dynamicContext.repFor(nameEntry.typ).expandedDatabaseColumns(label).map(_ -> nameEntry.name)
      }.map {
        case (label, name) => Doc(label) -> name
      }.toVector

      val valuesLabel = Doc(dynamicContext.gensymProvider.next())

      val selection = physicalSchema.map { case (physicalColName, outputName) =>
        valuesLabel ++ d"." ++ Doc(physicalColName) +#+ d"AS" +#+ Doc(physicalColName).annotate(SqlizeAnnotation.OutputName[MT](outputName))
      }.commaSep

      val rows = values.values.map { row =>
        row.toSeq.zipWithIndex.flatMap { case (expr, idx) =>
          val sqlized = exprSqlizer.sqlize(expr)

          if(compressedColumnIndices(idx)) sqlized.compressed.sqls
          else sqlized.sqls
        }.parenthesized
      }.toSeq.commaSep

      (
        Seq(
          Seq(d"SELECT", selection).vsep.nest(2),
          Seq(d"FROM (VALUES", rows).vsep.nest(2) ++ Doc.lineCat ++ d") AS" +#+ valuesLabel +#+ physicalSchema.map(_._1).parenthesized
        ).hsep,
        OrderedMap() ++ values.schema.iterator.zipWithIndex.map { case ((label, nameEntry), idx) =>
          label -> AugmentedType[MT](dynamicContext.repFor(nameEntry.typ), isExpanded = !compressedColumnIndices(idx))
        }
      )
    }
  }

  private def sqlizeSelect(
    select: Select,
    preSchemas: AvailableSchemas,
    dynamicContext: DynamicContext,
    topLevel: Boolean
  ): (Doc, AugmentedSchema) = {
    // ok so the order here is very particular because we need to
    // track column-compressedness.
    //
    // Which means we need to do FROM first to discover our
    // availableSchemas, then the SELECT list to find our
    // selectListIndices, then distinct-where-groupby-having-orderby (in any
    // order)

    var Select(
      distinct,
      selectList,
      from,
      where,
      groupBy,
      having,
      orderBy,
      limit,
      offset,
      search,
      hint
    ) = select

    if(search.isDefined) {
      throw new Exception("Should have rewritten search out of the query by the time sqlization happens")
    }

    if(topLevel) {
      // We'll want to sqlize selected geometries with an additional
      // "convert to WKB" wrapper, which means we can't use such
      // columns as select list references.
      val selectListIndexes = selectList.values.map(_.expr).toVector
      distinct = deSelectListReferenceWrappedToplevelExprs(dynamicContext, selectListIndexes, distinct)
      groupBy = groupBy.map(deSelectListReferenceWrappedToplevelExprs(dynamicContext, selectListIndexes, _))
      having = having.map(deSelectListReferenceWrappedToplevelExprs(dynamicContext, selectListIndexes, _))
      orderBy = orderBy.map(deSelectListReferenceWrappedToplevelExprs(dynamicContext, selectListIndexes, _))
    }

    val (fromSql, availableSchemas) = sqlizeFrom(from, preSchemas, dynamicContext)

    val selectListExprs: Vector[(AutoColumnLabel, (ExprSql, ColumnName))] = {
      val exprSqlizer = this.exprSqlizer.withContext(availableSchemas, Vector.empty, dynamicContext)
      selectList.iterator.map { case (label, namedExpr) =>
        val sqlized = exprSqlizer.sqlize(namedExpr.expr)
        val wrapped =
          if(topLevel) {
            dynamicContext.repFor(namedExpr.expr.typ).wrapTopLevel(sqlized)
          } else {
            sqlized
          }
        (label, (wrapped, namedExpr.name))
      }.toVector
    }

    val augmentedSchema = OrderedMap() ++ selectListExprs.iterator.map { case (label, (expr, _name)) =>
      val isExpanded =
        expr match {
          case _ : ExprSql.Compressed[_] => false
          case _ : ExprSql.Expanded[_] => true
        }
      (label : ColumnLabel) -> AugmentedType[MT](dynamicContext.repFor(expr.typ), isExpanded)
    }

    val selectListSql =
      selectListExprs.flatMap { case (label, (expr, outputName)) =>
        expr match {
          case cmp: ExprSql.Compressed[MT] =>
            Seq(cmp.sql +#+ d"AS" +#+ Doc(dynamicContext.repFor(cmp.typ).compressedDatabaseColumn(label)).annotate(SqlizeAnnotation.OutputName[MT](outputName)))
          case exp: ExprSql.Expanded[MT] =>
            val sqls = exp.sqls
            val colNames = dynamicContext.repFor(exp.typ).expandedDatabaseColumns(label)
            assert(sqls.length == colNames.length)
            sqls.lazyZip(colNames).map { (sql, name) =>
              sql +#+ d"AS" +#+ Doc(name).annotate(SqlizeAnnotation.OutputName[MT](outputName))
            }
        }
      }.commaSep

    val selectListIndices = selectListExprs.iterator.foldMap(1) { (idx, labelExpr) =>
      val (_label, (expr, _name)) = labelExpr
      (idx + expr.databaseExprCount, SelectListIndex(idx, isExpanded = false))
    }.toVector

    val exprSqlizer = this.exprSqlizer.withContext(availableSchemas, selectListIndices, dynamicContext)

    val distinctSql = sqlizeDistinct(distinct, exprSqlizer)

    val whereSql: Option[Doc] = where.map { w => Seq(d"WHERE", exprSqlizer.sqlize(w).compressed.sql).vsep.nest(2).group }

    val groupBySql =
      if(groupBy.nonEmpty) {
        Some(
          Seq(d"GROUP BY", groupBy.flatMap(exprSqlizer.sqlize(_).sqls).commaSep).vsep.nest(2).group
        )
      } else {
        None
      }
    val havingSql = having.map { w => Seq(d"HAVING", exprSqlizer.sqlize(w).compressed.sql).vsep.nest(2).group }
    val orderBySql =
      if(orderBy.nonEmpty) {
        Some(
          Seq(d"ORDER BY", orderBy.flatMap(exprSqlizer.sqlizeOrderBy(_).sqls).commaSep).vsep.nest(2).group
        )
      } else {
        None
      }
    val limitSql = limit.map { l => d"LIMIT" +#+ Doc(l.toString) }
    val offsetSql = offset.map { o => d"OFFSET" +#+ Doc(o.toString) }

    val selectionDoc = Seq(
      Some(d"SELECT"),
      distinctSql,
      Some(selectListSql).filter(_ => selectListExprs.nonEmpty)
    ).flatten.vsep.nest(2)

    val fromDoc = Seq(d"FROM", fromSql).vsep.nest(2).group

    val stmtSql = Seq(
      Some(selectionDoc),
      Some(fromDoc),
      whereSql,
      groupBySql,
      havingSql,
      orderBySql,
      limitSql,
      offsetSql
    ).flatten.vsep

    (stmtSql, augmentedSchema)
  }

  private def sqlizeDistinct(distinct: Distinctiveness, exprSqlizer: ExprSqlizer.Contexted[MT]): Option[Doc] = {
    distinct match {
      case Distinctiveness.Indistinct() => None
      case Distinctiveness.FullyDistinct() => Some(d"DISTINCT")
      case Distinctiveness.On(exprs) =>
        Some(
          Seq(d"DISTINCT ON (", exprs.flatMap(exprSqlizer.sqlize(_).sqls).commaSep).vsep.nest(2) ++ Doc.lineCat ++ d")"
        )
    }
  }

  private def sqlizeFrom(
    from: From,
    schemasSoFar: AvailableSchemas,
    dynamicContext: DynamicContext
  ): (Doc, AvailableSchemas) = {
    // Ok so this is _way_ looser than actual SQL visibility semantics
    // but because we've been sure to give every table-instance a
    // unique label, it doesn't matter.
    from.reduce[(Doc, AvailableSchemas)](
      sqlizeAtomicFrom(_, schemasSoFar, dynamicContext),
      { (acc, join) =>
        val (leftSql, leftSchemas) = acc
        val Join(joinType, lateral, _left, right, on) = join
        val (rightSql, schemasSoFar) = sqlizeAtomicFrom(right, leftSchemas, dynamicContext)
        val exprSqlizer = this.exprSqlizer.withContext(schemasSoFar, Vector.empty, dynamicContext)
        val onSql = exprSqlizer.sqlize(on).compressed.sql

        (
          Seq(
            leftSql,
            Seq(Some(sqlizeJoinType(joinType)), if(lateral) Some(d"LATERAL") else None, Some(rightSql)).flatten.vsep.nest(2),
            d"ON" +#+ onSql
          ).vsep,
          schemasSoFar
        )
      }
    )
  }

  private def sqlizeJoinType(joinType: JoinType): Doc = {
    val result =
      joinType match {
        case JoinType.Inner => "JOIN"
        case JoinType.LeftOuter => "LEFT OUTER JOIN"
        case JoinType.RightOuter => "RIGHT OUTER JOIN"
        case JoinType.FullOuter => "FULL OUTER JOIN"
      }
    Doc(result)
  }

  private def sqlizeAtomicFrom(from: AtomicFrom, availableSchemas: AvailableSchemas, dynamicContext: DynamicContext): (Doc, AvailableSchemas) = {
    val (sql, schema: AugmentedSchema) =
      from match {
        case FromTable(name, _rn, _alias, label, columns, _pks) =>
          (
            namespace.databaseTableName(name),
            OrderedMap() ++ columns.iterator.map { case (dcn, nameEntry) =>
              (dcn -> AugmentedType[MT](dynamicContext.repFor(nameEntry.typ), isExpanded = true))
            }
          )

        case FromStatement(stmt, _label, _rn, _alias) =>
          val (sql, schema) = sqlizeStatement(stmt, availableSchemas, dynamicContext, topLevel = false)
          (d"(" ++ sql ++ d")", schema)

        case FromSingleRow(_label, _alias) =>
          (d"(SELECT)", OrderedMap.empty)
      }

    (sql.annotate[SqlizeAnnotation](SqlizeAnnotation.Table(from.label)) +#+ d"AS" +#+ namespace.tableLabel(from.label), availableSchemas + (from.label -> schema))
  }

  private def deSelectListReferenceWrappedToplevelExprs(dynamicContext: DynamicContext, selectList: Vector[Expr], distinct: Distinctiveness): Distinctiveness =
    distinct match {
      case d@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => d
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(deSelectListReferenceWrappedToplevelExprs(dynamicContext, selectList, _)))
    }

  private def deSelectListReferenceWrappedToplevelExprs(dynamicContext: DynamicContext, selectList: Vector[Expr], orderBy: OrderBy): OrderBy =
    orderBy.copy(expr = deSelectListReferenceWrappedToplevelExprs(dynamicContext, selectList, orderBy.expr))

  private def deSelectListReferenceWrappedToplevelExprs(dynamicContext: DynamicContext, selectList: Vector[Expr], expr: Expr): Expr = {
    expr match {
      case SelectListReference(idx, isAggregated, isWindowed, typ) if dynamicContext.repFor(typ).hasTopLevelWrapper =>
        val replacement = selectList(idx - 1)
        assert(replacement.typ == typ)
        replacement
      case other =>
        other
    }
  }
}

object Sqlizer {
  case class DynamicContext[MT <: MetaTypes with MetaTypesExt](
    repFor: Rep.Provider[MT],
    provTracker: ProvenanceTracker[MT],
    physicalTableFor: Map[AutoTableLabel, types.DatabaseTableName[MT]],
    gensymProvider: GensymProvider,
    extraContext: MT#ExtraContext
  )

  def positionInfo[MT <: MetaTypes with MetaTypesExt](doc: SimpleDocStream[SqlizeAnnotation[MT]]): Array[Position] = {
    val positioner = new Positioner[MT]
    positioner.go(doc.asTree)
    positioner.builder.result()
  }

  private class Positioner[MT <: MetaTypes with MetaTypesExt] extends SqlizerUniverse[MT] {
    val builder = Array.newBuilder[Position]

    def go(node: SimpleDocTree[SqlizeAnnotation]): Unit =
      processNode(node, NoPosition)

    private def processNode(node: SimpleDocTree[SqlizeAnnotation], p: Position): Unit =
      node match {
        case tree.Empty => {}
        case tree.Char(_) =>
          builder += p
        case tree.Text(t) =>
          for(i <- 0 until t.length) builder += p
        case tree.Line(indent) =>
          builder += p
          for(i <- 0 until indent) builder += p
        case tree.Ann(SqlizeAnnotation.Expression(e), subtree) =>
          processNode(subtree, e.position.physicalPosition)
        case tree.Ann(SqlizeAnnotation.Table(_) | SqlizeAnnotation.OutputName(_) | SqlizeAnnotation.Custom(_), subtree) =>
          processNode(subtree, p)
        case tree.Concat(elems) =>
          for(elem <- elems) {
            processNode(elem, p)
          }
      }
  }

  case class Result[MT <: MetaTypes with MetaTypesExt](
    sql: Doc[SqlizeAnnotation[MT]],
    resultExtractor: ResultExtractor[MT],
    extraContextResult: MT#ExtraContextResult
  )
}
