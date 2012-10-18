package com.socrata.soql.analysis

import scala.util.parsing.input.Position

import com.socrata.soql.names.{ColumnName, FunctionName}
import com.socrata.soql.analysis.typed._

class AggregateInUngroupedContext(val function: FunctionName, val position: Position) extends Exception("Cannot use an aggregate function here:\n" + position.longString)
class ColumnNotInGroupBys(val column: ColumnName, val position: Position) extends Exception("Column not in group bys:\n" + position.longString)

class AggregateChecker[Type](aggregates: Set[MonomorphicFunction[Type]]) {
  type Expr = TypedFF[Type]

  /** Check that aggregates and column-names are used as appropriate for
    * the query.
    *
    * @param where The WHERE clause for the query, if present
    * @param orderBy The ORDER BY expressions for the query, minus sorting options
    * @param outputs The selected expressions for the query
    * @return `true` if this really was a groupless query or `false` if it's grouped by the empty list.
    */
  def apply(where: Option[Expr], orderBy: Seq[Expr], outputs: Seq[Expr]): Boolean = {
    where.foreach(checkPregroupExpression)
    try {
      orderBy.foreach(checkPregroupExpression)
      outputs.foreach(checkPregroupExpression)
      true
    } catch {
      case _: AggregateInUngroupedContext => // this is actually "group by {empty list}"
        apply(where, Nil, None, orderBy, outputs)
        false
    }
  }

  def apply(where: Option[Expr], groupBy: Seq[Expr], having: Option[Expr], orderBy: Seq[Expr], outputs: Seq[Expr]) {
    where.foreach(checkPregroupExpression)
    groupBy.foreach(checkPregroupExpression)
    having.foreach(checkPostgroupExpression(_, groupBy))
    orderBy.foreach(checkPostgroupExpression(_, groupBy))
    outputs.foreach(checkPostgroupExpression(_, groupBy))
  }

  def checkPregroupExpression(e: Expr) {
    e match {
      case FunctionCall(function, _) if aggregates.contains(function) =>
        throw new AggregateInUngroupedContext(function.name, e.position)
      case FunctionCall(_, params) =>
        params.foreach(checkPregroupExpression)
      case _: ColumnRef[_] | _: TypedLiteral[_] =>
        // ok, these are always good
    }
  }

  def checkPostgroupExpression(e: Expr, groupExpressions: Iterable[Expr]) {
    if(!isGroupExpression(e, groupExpressions)) {
      e match {
        case FunctionCall(function, params) if aggregates.contains(function) =>
          params.foreach(checkPregroupExpression)
        case FunctionCall(_, params) =>
          params.foreach(checkPostgroupExpression(_, groupExpressions))
        case _: TypedLiteral[_] =>
          // ok, this is always good
        case col: ColumnRef[_] =>
          throw new ColumnNotInGroupBys(col.column, col.position)
      }
    }
  }

  def isGroupExpression(e: TypedFF[Type], groupExpressions: Iterable[TypedFF[Type]]): Boolean =
    // This function doesn't take a Set because, while we're doing set-membership, we don't want to
    // re-compute our hashcodes over and over and over.  This should also be a "small" set.  So let's
    // just do a linear probe.
    groupExpressions.exists(_ == e)
}
