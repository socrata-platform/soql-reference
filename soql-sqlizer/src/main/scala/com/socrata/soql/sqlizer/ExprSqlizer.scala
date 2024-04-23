package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

object ExprSqlizer {
  trait Contexted[MT <: MetaTypes with MetaTypesExt] extends SqlizerUniverse[MT] {
    def sqlizeOrderBy(e: OrderBy): OrderBySql
    def sqlize(e: Expr): ExprSql
  }
}

class ExprSqlizer[MT <: MetaTypes with MetaTypesExt](
  val funcallSqlizer: FuncallSqlizer[MT],
  val exprSqlFactory: ExprSqlFactory[MT]
) extends SqlizerUniverse[MT] {
  protected class DefaultContextedExprSqlizer(
    availableSchemas: AvailableSchemas,
    selectListIndices: IndexedSeq[SelectListIndex],
    sqlizerCtx: Sqlizer.DynamicContext[MT]
  ) extends ExprSqlizer.Contexted[MT] {
    protected def sqlizeOrderBy(expr: ExprSql, ascending: Boolean, nullLast: Boolean): OrderBySql = {
      if(sqlizerCtx.repFor(expr.typ).isProvenanced) {
        if(!sqlizerCtx.provTracker(expr.expr).isPlural) {
          expr match {
            case result : ExprSql.Compressed[_] =>
              // Compressed, so we've already lost.  Just order by it.
              OrderBySql(result, ascending = ascending, nullLast = nullLast)
            case result : ExprSql.Expanded[_] =>
              // all provenance values in a column with non-plural
              // provenance will be the same same; eliminate them
              // from the generated sql so that the pg optimizer
              // doesn't have to.
              val Seq(_provenance, value) = result.sqls
              OrderBySql(exprSqlFactory(value, expr.expr), ascending = ascending, nullLast = nullLast)
          }
        } else {
          OrderBySql(expr, ascending = ascending, nullLast = nullLast)
        }
      } else {
        // We'll compress into a json array it because we don't want
        // ORDER BY's null positioning to be inconsistent depending on
        // whether or not it's expanded.
        //
        // One wrinkle here: if expr is an expanded compound column
        // which has been replaced with a select-list reference, we'll
        // have to undo that replacement because we can't generate
        //    compress(1, 2)
        // and have it work.
        val effectiveExpr =
          (expr, expr.expr) match {
            case (expanded: ExprSql.Expanded[MT], ser: SelectListReference) =>
              selectListIndices(ser.index - 1).exprSql
            case (other, _) =>
              other
          }
        OrderBySql(effectiveExpr.compressed, ascending = ascending, nullLast = nullLast)
      }
    }

    override def sqlizeOrderBy(e: OrderBy): OrderBySql =
      sqlizeOrderBy(sqlize(e.expr), ascending = e.ascending, nullLast = e.nullLast)

    override def sqlize(e: Expr): ExprSql = {
      e match {
        case pc@PhysicalColumn(tbl, _tableName, col, typ) =>
          val trueType = availableSchemas(tbl)(col)
          assert(trueType.typ == typ)
          assert(trueType.isExpanded)
          sqlizerCtx.repFor(typ).physicalColumnRef(pc)
        case vc@VirtualColumn(tbl, col, typ) =>
          val trueType = availableSchemas(tbl)(col)
          assert(trueType.typ == typ)
          sqlizerCtx.repFor(typ).virtualColumnRef(vc, isExpanded = trueType.isExpanded)
        case nl@NullLiteral(typ) =>
          sqlizerCtx.repFor(typ).nullLiteral(nl)
        case lit@LiteralValue(v) =>
          sqlizerCtx.repFor(lit.typ).literal(lit)
        case SelectListReference(n, _isAggregated, _isWindowed, typ) =>
          val SelectListIndex(startingPhysicalColumn, exprSql) = selectListIndices(n-1)
          exprSqlFactory(
            (0 until exprSql.sqls.length).map { offset =>
              Doc((startingPhysicalColumn + offset).toString)
            },
            e
          )
        case fc@FunctionCall(_func, args) =>
          funcallSqlizer.sqlizeOrdinaryFunction(fc, args.map(sqlize), sqlizerCtx)
        case afc@AggregateFunctionCall(_func, args, _distinct, filter) =>
          funcallSqlizer.sqlizeAggregateFunction(afc, args.map(sqlize), filter.map(sqlize), sqlizerCtx)
        case wfc@WindowedFunctionCall(_func, args, filter, partitionBy, orderBy, _frame) =>
          funcallSqlizer.sqlizeWindowedFunction(
            wfc,
            args.map(sqlize),
            filter.map(sqlize),
            partitionBy.map(sqlize),
            orderBy.map(sqlizeOrderBy),
            sqlizerCtx
          )
      }
    }
  }

  def withContext(
    availableSchemas: AvailableSchemas,
    selectListIndices: IndexedSeq[SelectListIndex],
    sqlizerCtx: Sqlizer.DynamicContext[MT]
  ): ExprSqlizer.Contexted[MT] =
    new DefaultContextedExprSqlizer(availableSchemas, selectListIndices, sqlizerCtx)
}
