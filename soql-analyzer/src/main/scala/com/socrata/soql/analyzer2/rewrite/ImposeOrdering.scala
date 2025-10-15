package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class ImposeOrdering[MT <: MetaTypes] private (labelProvider: LabelProvider, isOrderable: MT#ColumnType => Boolean) extends StatementUniverse[MT] {
  def rewriteStatement(stmt: Statement): Statement = {
    stmt match {
      case ct@CombinedTables(op, left, right) =>
        // Need to introduce a new layer of select just for the
        // ordering...

        val newTableLabel = labelProvider.tableLabel()

        // If this CombinedTables has a fully-orderable set of unique
        // columns, we can use them for an order-by.  Otherwise we'll
        // just order by all that is orderable.
        val usefulUnique = ct.unique.find(_.forall { c => isOrderable(ct.schema(c).typ) })

        def orderByOrderable(predicate: (AutoColumnLabel, CT) => Boolean) =
          ct.schema.iterator.collect { case (columnLabel, Statement.SchemaEntry(_, typ, _hint, _isSynthetic)) if predicate(columnLabel, typ) =>
            assert(isOrderable(typ))
            OrderBy(VirtualColumn[MT](newTableLabel, columnLabel, typ)(AtomicPositionInfo.Synthetic), true, true)
          }.to(Vector)

        def newOrderBy = usefulUnique match {
          case Some(cols) =>
            val colSet = cols.toSet
            orderByOrderable { (col, _) => colSet(col) }
          case None =>
            orderByOrderable { (_, t) => isOrderable(t) }
        }

        Select(
          Distinctiveness.Indistinct(),
          OrderedMap() ++ ct.schema.iterator.map { case (columnLabel, Statement.SchemaEntry(name, typ, hint, isSynthetic)) =>
            labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newTableLabel, columnLabel, typ)(AtomicPositionInfo.Synthetic), name, hint = None, isSynthetic = isSynthetic)
          },
          FromStatement(ct, newTableLabel, None, None, None),
          None,
          Nil,
          None,
          newOrderBy,
          None,
          None,
          None,
          Set.empty
        )

      case cte@CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        cte.copy(useQuery = rewriteStatement(useQuery))

      case v@Values(_, _) =>
        // This cannot be a top-level thing; if we ever want to impose
        // an ordering on a Values, it too will have to get rewritten
        // into a select-that-imposes-an-order (probably by adding an
        // index column to the values, ordering by that, and selecting
        // all except that).  See also similar comment over in
        // PreserveOrdering.
        v

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val allUnique: Seq[Seq[Expr]] =
          if(select.isAggregated) {
            if(select.groupBy.isEmpty) {
              return select // no group by: we're producing a single row and so don't need to change anything at all
            } else {
              // `select.unique` will include group-by exprs _if
              // they're all selected_, but we can usefully order by
              // them even if they're not, so prepend that to the
              // list of things to try.
              Seq(groupBy) ++ select.unique.map(_.map(selectList(_).expr))
            }
          } else {
            from.unique
          }
        val usefulUnique = allUnique.find(_.forall { c => isOrderable(c.typ) })

        val existingOrderBy = orderBy.map(_.expr).to(Set)

        def allOrderableSelectedCols(except: Expr => Boolean): Iterator[OrderBy] =
          selectList.valuesIterator.collect { case NamedExpr(expr, name, _hint, _isSynthetic) if isOrderable(expr.typ) && !except(expr) =>
            OrderBy(expr, true, true)
          }

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

              val afterPreexisting =
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

              val newBase = preexisting ++ afterPreexisting
              val newBaseSet = newBase.iterator.map(_.expr).to(Set)

              newBase ++ allOrderableSelectedCols(except = newBaseSet)
            case Distinctiveness.FullyDistinct() =>
              // fully distinct order by clauses must appear in the
              // select list; fortunately, that's where we're pulling
              // them from.
              orderBy ++ allOrderableSelectedCols(except = existingOrderBy)
            case Distinctiveness.Indistinct() =>
              val additional = usefulUnique match {
                case None => allOrderableSelectedCols(except = existingOrderBy)
                case Some(exprs) => exprs.filterNot(existingOrderBy).map(OrderBy(_, true, true))
              }
              orderBy ++ additional
          }
        select.copy(orderBy = newOrderBy.to(Vector))
    }
  }
}

object ImposeOrdering {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, isOrderable: MT#ColumnType => Boolean, stmt: Statement[MT]): Statement[MT] =
    new ImposeOrdering[MT](labelProvider, isOrderable).rewriteStatement(stmt)
}
