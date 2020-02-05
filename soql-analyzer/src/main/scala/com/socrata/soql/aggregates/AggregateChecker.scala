package com.socrata.soql.aggregates

import com.socrata.soql.typed._
import com.socrata.soql.exceptions.{AggregateInUngroupedContext, ColumnNotInGroupBys}
import com.socrata.soql.environment.{ColumnName, Qualified}

class AggregateChecker[Type] {
  type Expr = CoreExpr[Qualified[ColumnName], Type]

  /** Check that aggregates and column-names are used as appropriate for
    * the query.
    *
    * @param outputs The selected expressions for the query
    * @param where The WHERE clause for the query, if present
    * @param groupBy The GROUP BY expressions, if the clause is present
    * @param having The HAVING clause for the query, if present
    * @param orderBy The ORDER BY expressions for the query, minus sorting options
    *
    * @return `true` if this was a grouped query or `false` otherwise.
    * @throws AggregateInUngroupedContext if an aggregate function occurred in an illegal position
    * @throws ColumnNotInGroupBys if a reference to a column is found in an expression that is ''not''
    *                             in the GROUP BY expressions in a location where only aggregate expressions
    *                             are allowed.
    */
  def apply(outputs: Seq[Expr], where: Option[Expr], groupBy: Seq[Expr], having: Option[Expr], orderBy: Seq[Expr]): Boolean = {
    if(groupBy.nonEmpty || having.isDefined) { // ok, definitely a grouped query
      checkGrouped(outputs, where, groupBy, having, orderBy)
      true
    } else {
      // neither GROUP BY nor HAVING are set.  It could still be a grouped
      // query if there are aggregate calls in outputs or order by though...
      try {
        outputs.foreach(checkPregroupExpression("selected columns", _))
        orderBy.foreach(checkPregroupExpression("ORDER BY", _))
      } catch {
        case _: AggregateInUngroupedContext =>
          checkGrouped(outputs, where, Nil, None, orderBy)
          return true
      }
      where.foreach(checkPregroupExpression("WHERE", _))
      false
    }
  }

  def checkGrouped(outputs: Seq[Expr], where: Option[Expr], groupBy: Seq[Expr], having: Option[Expr], orderBy: Seq[Expr]) {
    outputs.foreach(checkPostgroupExpression("selected columns", _, groupBy))
    where.foreach(checkPregroupExpression("WHERE", _))
    groupBy.foreach(checkPregroupExpression("GROUP BY", _))
    having.foreach(checkPostgroupExpression("HAVING", _, groupBy))
    orderBy.foreach(checkPostgroupExpression("ORDER BY", _, groupBy))
  }

  def checkPregroupExpression(clause: String, e: Expr) {
    e match {
      case FunctionCall(function, _) if function.isWindowFunction =>
      case FunctionCall(function, _) if function.isAggregate =>
        throw AggregateInUngroupedContext(function.name, clause, e.position)
      case FunctionCall(_, params) =>
        params.foreach(checkPregroupExpression(clause, _))
      case _: ColumnRef[_, _] | _: TypedLiteral[_] =>
        // ok, these are always good
    }
  }

  def checkPostgroupExpression(clause: String, e: Expr, groupExpressions: Iterable[Expr]) {
    if(!isGroupExpression(e, groupExpressions)) {
      e match {
        case FunctionCall(function, params) if function.isWindowFunction =>
          // Skip the first parameter which is supposedly an aggregate function.
          params.tail.foreach(checkPregroupExpression(clause, _))
        case FunctionCall(function, params) if function.isAggregate =>
          params.foreach(checkPregroupExpression(clause, _))
        case FunctionCall(_, params) =>
          params.foreach(checkPostgroupExpression(clause, _, groupExpressions))
        case _: TypedLiteral[_] =>
          // ok, this is always good
        case col: ColumnRef[Qualified[ColumnName], _] =>
          throw ColumnNotInGroupBys(col.column.columnName, col.position)
      }
    }
  }

  def isGroupExpression(e: Expr, groupExpressions: Iterable[Expr]): Boolean =
    // This function doesn't take a Set because, while we're doing set-membership, we don't want to
    // re-compute our hashcodes over and over and over.  This should also be a "small" set.  So let's
    // just do a linear probe.
    groupExpressions.exists(_ == e)
}
