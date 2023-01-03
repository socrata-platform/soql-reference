package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class ImposeOrdering[RNS, CT, CV] private (labelProvider: LabelProvider, isOrderable: CT => Boolean) {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type Select = analyzer2.Select[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]
  type OrderBy = analyzer2.OrderBy[CT, CV]

  def rewriteStatement(stmt: Statement): Statement = {
    stmt match {
      case ct@CombinedTables(op, left, right) =>
        // Need to introduce a new layer of select just for the
        // ordering...

        val newTableLabel = labelProvider.tableLabel()

        Select(
          Distinctiveness.Indistinct,
          OrderedMap() ++ ct.schema.iterator.map { case (columnLabel, NameEntry(name, typ)) =>
            labelProvider.columnLabel() -> NamedExpr(Column(newTableLabel, columnLabel, typ)(AtomicPositionInfo.None), name)
          },
          FromStatement(ct, newTableLabel, None, None),
          None,
          Nil,
          None,
          ct.schema.iterator.collect { case (columnLabel, NameEntry(_, typ)) if isOrderable(typ) =>
            OrderBy(Column(newTableLabel, columnLabel, typ)(AtomicPositionInfo.None), true, true)
          }.to(Vector),
          None,
          None,
          None,
          Set.empty
        )

      case cte@CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        cte.copy(useQuery = rewriteStatement(useQuery))

      case v@Values(_) =>
        // This cannot be a top-level thing; if we ever want to impose
        // an ordering on a Values, it too will have to get rewritten
        // into a select-that-imposes-an-order (probably by adding an
        // index column to the values, ordering by that, and selecting
        // all except that).  See also similar comment over in
        // PreserveOrdering.
        v

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val existingOrderBy = orderBy.map(_.expr).to(Set)

        val newOrderBy: Seq[OrderBy] =
          distinctiveness match {
            case Distinctiveness.On(exprs) =>
              // Ok, this is the weird case.  We need to ensure that
              // all exprs in the Distinctiveness clause come first in
              // our generated ORDER BY, but we also want to preserve
              // the ordering that the user specified.  Fortunately
              // what we know is that if we encounter an ORDER BY
              // that's _not_ in this set, we're done...
              val distinctiveExprs = exprs.to(Set)

              val preexisting = orderBy.takeWhile { ob => distinctiveExprs(ob.expr) }
              val additional = orderBy.drop(preexisting.length)

              val afterPrexisting =
                if(additional.isEmpty) {
                  // We _might_ need to add additional exprs that
                  // aren't in the order-by list yet.
                  val notYetOrderedBy = preexisting.iterator.map(_.expr).to(Set)
                  exprs.filterNot(notYetOrderedBy).map(OrderBy(_, true, true))
                } else {
                  // Since there were already order by clauses not in
                  // the distinct-list, we don't need to add anything
                  // that wasn't already there.
                  additional
                }

              val newBase = preexisting ++ afterPrexisting
              val newBaseSet = newBase.iterator.map(_.expr).to(Set)

              newBase ++ selectList.valuesIterator.collect { case NamedExpr(expr, name) if isOrderable(expr.typ) && !newBaseSet(expr) =>
                OrderBy(expr, true, true)
              }
            case Distinctiveness.FullyDistinct | Distinctiveness.Indistinct =>
              // fully distinct order by clauses must appear in the
              // select list; fortunately, that's where we're pulling
              // them from.
              orderBy ++ selectList.valuesIterator.collect { case NamedExpr(expr, name) if isOrderable(expr.typ) && !existingOrderBy(expr) =>
                OrderBy(expr, true, true)
              }
          }
        select.copy(orderBy = newOrderBy.to(Vector))
    }
  }
}

object ImposeOrdering {
  def apply[RNS, CT, CV](labelProvider: LabelProvider, isOrderable: CT => Boolean, stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] =
    new ImposeOrdering[RNS, CT, CV](labelProvider, isOrderable).rewriteStatement(stmt)
}
