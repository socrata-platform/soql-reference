package com.socrata.soql.analyzer2
package rewrite

import com.socrata.soql.collection._

trait RewritePassMixin[MT <: MetaTypes] extends StatementUniverse[MT] {
  // This rewrites all the subqueries in expression-position in the
  // statement, _without_ recursing into parent queries
  protected def rewriteExpressionSubqueries[MT <: MetaTypes](s: Statement, f: Statement => Statement): Statement =
    s match {
      case ct: CombinedTables =>
        ct
      case cte: CTE =>
        cte
      case Values(labels, values) =>
        Values(labels, values.map(_.map(rewriteExpressionSubqueries(_, f))))
      case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        Select(
          distinctiveness match {
            case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() =>
              distinctiveness
            case Distinctiveness.On(exprs) =>
              Distinctiveness.On(exprs.map(rewriteExpressionSubqueries(_, f)))
          },
          selectList.withValuesMapped(rewriteExpressionSubqueries(_, f)),
          from.map(
            identity,
            { (joinType, lateral, left, right, on) =>
              Join(joinType, lateral, left, right, rewriteExpressionSubqueries(on, f))
            }
          ),
          where.map(rewriteExpressionSubqueries(_, f)),
          groupBy.map(rewriteExpressionSubqueries(_, f)),
          having.map(rewriteExpressionSubqueries(_, f)),
          orderBy.map(rewriteExpressionSubqueries(_, f)),
          limit,
          offset,
          search,
          hint
        )
    }

  protected def rewriteExpressionSubqueries[MT <: MetaTypes](ne: NamedExpr, f: Statement => Statement): NamedExpr =
    ne.copy(expr = rewriteExpressionSubqueries(ne.expr, f))

  protected def rewriteExpressionSubqueries[MT <: MetaTypes](ob: OrderBy, f: Statement => Statement): OrderBy =
    ob.copy(expr = rewriteExpressionSubqueries(ob.expr, f))

  protected def rewriteExpressionSubqueries[MT <: MetaTypes](e: Expr, f: Statement => Statement): Expr =
    e match {
      case atomic: AtomicExpr =>
        atomic
      case fc@FunctionCall(name, args) =>
        FunctionCall(name, args.map(rewriteExpressionSubqueries(_, f)))(fc.position)
      case afc@AggregateFunctionCall(name, args, distinct, filter) =>
        AggregateFunctionCall(name, args.map(rewriteExpressionSubqueries(_, f)), distinct, filter.map(rewriteExpressionSubqueries(_, f)))(afc.position)
      case wfc@WindowedFunctionCall(name, args, filter, partitionBy, orderBy, frame) =>
        WindowedFunctionCall(name, args.map(rewriteExpressionSubqueries(_, f)), filter.map(rewriteExpressionSubqueries(_, f)), partitionBy.map(rewriteExpressionSubqueries(_, f)), orderBy.map(rewriteExpressionSubqueries(_, f)), frame)(wfc.position)
      case in@InSubselect(scrutinee, not, subquery, typ) =>
        InSubselect(rewriteExpressionSubqueries(scrutinee, f), not, f(subquery), typ)(in.position)
    }
}
