package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveTrivialSelects[MT <: MetaTypes] private () extends StatementUniverse[MT] {
  // A "trivial select" is a select of the form:
  //   select column_ref, ... from atomicfrom
  // without any other modifiers and where all the column_refs point
  // into the atomicfrom.

  object TrivialSelect {
    def unapply(stmt: Statement): Option[(OrderedMap[AutoColumnLabel, Column], AtomicFrom)] = {
      rewriteStatement(stmt) match {
        case Select(Distinctiveness.Indistinct(), selectList, from: AtomicFrom, None, Nil, None, Nil, None, None, None, hints) if hints.isEmpty =>
          var exprs = OrderedMap[AutoColumnLabel, Column]()
          for((label, namedExpr) <- selectList) {
            namedExpr.expr match {
              case c: Column if c.table == from.label => exprs += label -> c
              case _ => return None
            }
          }
          Some((exprs, from))
        case _ =>
          None
      }
    }
  }

  def rewriteStatement(stmt: Statement): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left), rewriteStatement(right))

      case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        val newDefQuery = rewriteStatement(defQuery)
        val newUseQuery = rewriteStatement(useQuery)
        CTE(defLabel, defAlias, newDefQuery, materializedHint, newUseQuery)

      case v@Values(_, _) =>
        v

      case Select(distinctiveness, selectList, from@FromStatement(TrivialSelect(subSelectList, subFrom), fromLabel, _resourceName, _alias), where, groupBy, having, orderBy, limit, offset, None /* search is weird, so we'll only do this if it doesn't exist */, hint) =>
        val columnMap: Map[VirtualColumn, Expr] = subSelectList.iterator.map { case (columnLabel, column) =>
          VirtualColumn(fromLabel, columnLabel, column.typ)(AtomicPositionInfo.None) -> column
        }.toMap

        Select(
          rewriteDistinctiveness(distinctiveness, columnMap),
          OrderedMap() ++ selectList.iterator.map { case (label, NamedExpr(expr, name)) =>
            (label, NamedExpr(rewriteExpr(expr, columnMap), name))
          },
          subFrom,
          where.map(rewriteExpr(_, columnMap)),
          groupBy.map(rewriteExpr(_, columnMap)),
          having.map(rewriteExpr(_, columnMap)),
          orderBy.map(rewriteOrderBy(_, columnMap)),
          limit,
          offset,
          None,
          hint
        )

      case select: Select =>
        select.copy(from = rewriteFrom(select.from))
    }
  }

  def rewriteDistinctiveness(d: Distinctiveness, cm: Map[VirtualColumn, Expr]): Distinctiveness = {
    d match {
      case noSubExprs@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => noSubExprs
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(rewriteExpr(_, cm)))
    }
  }

  def rewriteExpr(e: Expr, cm: Map[VirtualColumn, Expr]): Expr = {
    e match {
      case vc: VirtualColumn => cm.getOrElse(vc, vc)
      case otherAtomic: AtomicExpr => otherAtomic
      case fc@FunctionCall(func, args) =>
        FunctionCall(func, args.map(rewriteExpr(_, cm)))(fc.position)
      case afc@AggregateFunctionCall(func, args, distinct, filter) =>
        AggregateFunctionCall(func, args.map(rewriteExpr(_, cm)), distinct, filter.map(rewriteExpr(_, cm)))(afc.position)
      case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
        WindowedFunctionCall(
          func,
          args.map(rewriteExpr(_, cm)),
          filter.map(rewriteExpr(_, cm)),
          partitionBy.map(rewriteExpr(_, cm)),
          orderBy.map(rewriteOrderBy(_, cm)),
          frame
        )(wfc.position)
    }
  }

  def rewriteOrderBy(ob: OrderBy, cm: Map[VirtualColumn, Expr]): OrderBy = {
    ob.copy(expr = rewriteExpr(ob.expr, cm))
  }

  def rewriteFrom(from: From): From = {
    from.map[MT](
      rewriteAtomicFrom(_),
      { (joinType, lateral, left, right, on) =>
        val newRight = rewriteAtomicFrom(right)
        Join(joinType, lateral, left, newRight, on)
      }
    )
  }

  def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
    from match {
      case ft: FromTable => ft
      case fsr: FromSingleRow => fsr
      case fs: FromStatement => fs.copy(statement = rewriteStatement(fs.statement))
    }
  }
}

/** Remove trivial selects, where "trivial" means "only selects
  * columns from some atomic from, with no other action". */
object RemoveTrivialSelects {
  def apply[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    new RemoveTrivialSelects[MT]().rewriteStatement(stmt)
  }
}
