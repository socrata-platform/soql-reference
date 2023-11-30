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

        def orderByOrderable(predicate: (AutoColumnLabel, CT) => Boolean) =
          ct.schema.iterator.collect { case (columnLabel, Statement.SchemaEntry(_, typ, _isSynthetic)) if predicate(columnLabel, typ) =>
            assert(isOrderable(typ))
            OrderBy(VirtualColumn[MT](newTableLabel, columnLabel, typ)(AtomicPositionInfo.None), true, true)
          }.to(Vector)

        def orderAllThatIsOrderable = orderByOrderable { (_, t) => isOrderable(t) }

        def newOrderBy = op match {
          case TableFunc.Union | TableFunc.UnionAll =>
            orderAllThatIsOrderable

          case TableFunc.Intersect | TableFunc.IntersectAll | TableFunc.Minus | TableFunc.MinusAll =>
            // We'll only be returning rows that exist in the left
            // subquery's output, so we can use the left query's
            // unique columns to provide an ordering.

            val leftUniqueOrderable =
              left.unique.map { cols =>
                (cols.toSet, cols.map { col => left.schema(col) })
              }.filter { case (_, schemaEntries) =>
                schemaEntries.forall { se => isOrderable(se.typ) }
              }

            leftUniqueOrderable.headOption match {
              case Some((cols, _schemaEntries)) =>
                val colSet = cols.toSet
                orderByOrderable { (col, _) => colSet(col) }
              case None =>
                orderAllThatIsOrderable
            }
        }

        Select(
          Distinctiveness.Indistinct(),
          OrderedMap() ++ ct.schema.iterator.map { case (columnLabel, Statement.SchemaEntry(name, typ, isSynthetic)) =>
            labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newTableLabel, columnLabel, typ)(AtomicPositionInfo.None), name, isSynthetic = isSynthetic)
          },
          FromStatement(ct, newTableLabel, None, None),
          None,
          Nil,
          None,
          // This is pretty simplistic; it just says "order by all
          // orderable columns" which works, but we can do better if
          // we use knowledge about `op` (e.g., for Intersect and
          // Minus we can use left.unique if one exists, and for union
          // we can use left.unique ++ right.unique)
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
        val allUnique =
          if(select.isAggregated) {
            select.unique.map(_.map(selectList(_).expr))
          } else {
            from.unique
          }
        val usefulUnique = allUnique.filter(_.forall { c => isOrderable(c.typ) })

        val existingOrderBy = orderBy.map(_.expr).to(Set)

        def allOrderableSelectedCols(except: Expr => Boolean): Iterator[OrderBy] =
          selectList.valuesIterator.collect { case NamedExpr(expr, name, _isSynthetic) if isOrderable(expr.typ) && !except(expr) =>
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
              val additional = usefulUnique.headOption match {
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
