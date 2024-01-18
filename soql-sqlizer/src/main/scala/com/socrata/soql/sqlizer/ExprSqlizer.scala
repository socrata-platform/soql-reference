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
  protected val funcallSqlizer: FuncallSqlizer[MT],
  protected val exprSqlFactory: ExprSqlFactory[MT]
) extends SqlizerUniverse[MT] {
  protected class DefaultContextedExprSqlizer(
    availableSchemas: AvailableSchemas,
    selectListIndices: IndexedSeq[SelectListIndex],
    sqlizerCtx: Sqlizer.DynamicContext[MT]
  ) extends ExprSqlizer.Contexted[MT] {
    override def sqlizeOrderBy(e: OrderBy): OrderBySql = {
      if(sqlizerCtx.repFor(e.expr.typ).isProvenanced) {
        if(!sqlizerCtx.provTracker(e.expr).isPlural) {
          sqlize(e.expr) match {
            case result : ExprSql.Compressed[_] =>
              // Compressed, so we've already lost.  Just order by it.
              OrderBySql(result, ascending = e.ascending, nullLast = e.nullLast)
            case result : ExprSql.Expanded[_] =>
              // all provenance values in a column with non-plural
              // provenance will be the same same; eliminate them
              // from the generated sql so that the pg optimizer
              // doesn't have to.
              val Seq(_provenance, value) = result.sqls
              OrderBySql(exprSqlFactory(value, e.expr), ascending = e.ascending, nullLast = e.nullLast)
          }
        } else {
          OrderBySql(sqlize(e.expr), ascending = e.ascending, nullLast = e.nullLast)
        }
      } else {
        // We'll compress into a json array it because we don't want
        // ORDER BY's null positioning to be inconsistent depending on
        // whether or not it's expanded
        OrderBySql(sqlize(e.expr).compressed, ascending = e.ascending, nullLast = e.nullLast)
      }
    }

    override def sqlize(e: Expr): ExprSql =
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
          val SelectListIndex(startingPhysicalColumn, isExpanded) = selectListIndices(n-1)
          if(isExpanded) {
            exprSqlFactory(Doc(startingPhysicalColumn.toString), e)
          } else {
            exprSqlFactory(
              (0 until sqlizerCtx.repFor(typ).expandedColumnCount).map { offset =>
                Doc((startingPhysicalColumn + offset).toString)
              },
              e
            )
          }
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

  def withContext(
    availableSchemas: AvailableSchemas,
    selectListIndices: IndexedSeq[SelectListIndex],
    sqlizerCtx: Sqlizer.DynamicContext[MT]
  ): ExprSqlizer.Contexted[MT] =
    new DefaultContextedExprSqlizer(availableSchemas, selectListIndices, sqlizerCtx)
}
