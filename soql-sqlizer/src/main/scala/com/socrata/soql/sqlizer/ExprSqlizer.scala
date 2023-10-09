package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

class ExprSqlizer[MT <: MetaTypes with MetaTypesExt](
  sqlizer: Sqlizer[MT],
  availableSchemas: Map[AutoTableLabel, Map[types.ColumnLabel[MT], AugmentedType[MT]]],
  selectListIndices: IndexedSeq[SelectListIndex],
  dynamicContext: Sqlizer.DynamicContext[MT]
) extends StatementUniverse[MT] {
  import sqlizer.funcallSqlizer
  import sqlizer.exprSqlFactory

  private val repFor = dynamicContext.repFor

  def sqlizeOrderBy(e: OrderBy): OrderBySql[MT] = {
    if(repFor(e.expr.typ).isProvenanced) {
      if(!dynamicContext.provTracker(e.expr).isPlural) {
        // all provenance values in a physical column will be the
        // same; eliminate them from the sqlizer so that the pg
        // optimizer doesn't have to.
        val result = sqlize(e.expr)
        val Seq(_provenance, value) = result.sqls
        OrderBySql(exprSqlFactory(value, e.expr), ascending = e.ascending, nullLast = e.nullLast)
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

  def sqlize(e: Expr): ExprSql[MT] =
    e match {
      case pc@PhysicalColumn(tbl, _tableName, col, typ) =>
        val trueType = availableSchemas(tbl)(col)
        assert(trueType.typ == typ)
        assert(trueType.isExpanded)
        repFor(typ).physicalColumnRef(pc)
      case vc@VirtualColumn(tbl, col, typ) =>
        val trueType = availableSchemas(tbl)(col)
        assert(trueType.typ == typ)
        repFor(typ).virtualColumnRef(vc, isExpanded = trueType.isExpanded)
      case nl@NullLiteral(typ) =>
        repFor(typ).nullLiteral(nl)
      case lit@LiteralValue(v) =>
        repFor(lit.typ).literal(lit)
      case SelectListReference(n, _isAggregated, _isWindowed, typ) =>
        val SelectListIndex(startingPhysicalColumn, isExpanded) = selectListIndices(n-1)
        if(isExpanded) {
          exprSqlFactory(Doc(startingPhysicalColumn.toString), e)
        } else {
          exprSqlFactory(
            (0 until repFor(typ).expandedColumnCount).map { offset =>
              Doc((startingPhysicalColumn + offset).toString)
            },
            e
          )
        }
      case fc@FunctionCall(_func, args) =>
        funcallSqlizer.sqlizeOrdinaryFunction(fc, args.map(sqlize), dynamicContext)
      case afc@AggregateFunctionCall(_func, args, _distinct, filter) =>
        funcallSqlizer.sqlizeAggregateFunction(afc, args.map(sqlize), filter.map(sqlize), dynamicContext)
      case wfc@WindowedFunctionCall(_func, args, filter, partitionBy, orderBy, _frame) =>
        funcallSqlizer.sqlizeWindowedFunction(
          wfc,
          args.map(sqlize),
          filter.map(sqlize),
          partitionBy.map(sqlize),
          orderBy.map(sqlizeOrderBy),
          dynamicContext
        )
    }
}
