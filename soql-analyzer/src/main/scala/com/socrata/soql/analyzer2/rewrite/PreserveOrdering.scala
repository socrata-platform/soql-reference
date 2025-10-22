package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._
import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.MonomorphicFunction

class PreserveOrdering[MT <: MetaTypes] private (provider: LabelProvider) extends StatementUniverse[MT] {
  type ACTEs = AvailableCTEs[MT, Seq[(AutoColumnLabel, CT, Boolean, Boolean)]]

  // "wantOutputOrdered" == "if this statement can be rewritten to
  // preserve the ordering of its underlying query, do so".
  // "wantOrderingColumns" == "The caller needs column from this table
  // to order itself".  Note that just because the caller wants a
  // thing, it will not necessarily get it!
  def rewriteStatement(availableCTEs: ACTEs, stmt: Statement, wantOutputOrdered: Boolean, wantOrderingColumns: Boolean): (Seq[(AutoColumnLabel, CT, Boolean, Boolean)], Statement) = {
    stmt match {
      case CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        (Nil, CombinedTables(op, rewriteStatement(availableCTEs, left, false, false)._2, rewriteStatement(availableCTEs, right, false, false)._2))

      case CTE(defns, useQuery) =>
        val (newAvailableCTEs, newDefinitions) = availableCTEs.collect(defns) { (aCTEs, query) =>
          rewriteStatement(aCTEs, query, true, true)
        }
        val (orderingColumns, newUseQuery) = rewriteStatement(newAvailableCTEs, useQuery, wantOutputOrdered, wantOrderingColumns)

        (
          orderingColumns,
          CTE(newDefinitions, newUseQuery)
        )

      case v@Values(_, _) =>
        // TODO, should this rewrite into a SELECT ... FROM
        // (values...) ORDER BY synthetic_column ?  If so, we'll need
        // a way to generate synthetic_column (== have a way to turn
        // an integer into a CV).  See also similar comment over in
        // ImposeOrdering.
        (Nil, v)

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val usedNames = selectList.valuesIterator.map(_.name).to(scm.HashSet)
        def freshName(base: String) = {
          val created = Iterator.from(1).map { i => ColumnName(base + "_" + i) }.find { n =>
            !usedNames.contains(n)
          }.get
          usedNames += created
          created
        }

        val wantSubqueryOrdered = (wantOutputOrdered || select.isWindowed) && !select.isAggregated && distinctiveness == Distinctiveness.Indistinct()
        val (extraOrdering, newFrom) = rewriteFrom(availableCTEs, from, wantSubqueryOrdered, wantSubqueryOrdered)

        // We will at the very least want to add the ordering
        // columns from the subquery onto the end of our order-by...
        val orderedSelf = locally {
          // If we didn't request that the subquery be ordered, then
          // we'll ignore any extra ordering info it returned to us.
          // It _shouldn't_ ever do so in that case, but we'll guard
          // it just in case, since it's required for correctness (in
          // the face of aggregates and DISTINCT ON) that we not have
          // the extra ordering.
          val orderByFromSubquery =
            if(wantSubqueryOrdered) {
              // if we've already ordered by this expr (asc/desc and
              // null positioning don't matter) then don't re-order by
              // it at a later stage.
              val alreadyOrderedBy = orderBy.iterator.map(_.expr).toSet
              extraOrdering.filterNot { ob => alreadyOrderedBy(ob.expr) }
            } else {
              Nil
            }
          select.copy(
            from = newFrom,
            orderBy = orderBy ++ orderByFromSubquery
          )
        }
        if(wantOrderingColumns) {
          // ..and our caller wants to know how we're ordered, so make
          // sure we've put the relevant expressions in our select
          // list and return them.  Note that if we're fully DISTINCT,
          // any order-by clauses must already be in our select list,
          // so they'll just get collected into outputInfo but we
          // won't actually create any newColumns.
          val (newColumns, outputInfo) = orderedSelf.orderBy.map { case OrderBy(expr, asc, nullLast) =>
            selectList.find { case (label, NamedExpr(e, _, _, _)) => expr == e } match {
              case None =>
                val columnLabel = provider.columnLabel()
                (Some(columnLabel -> NamedExpr(expr, freshName("order"), hint = None, isSynthetic = true)), (columnLabel, expr.typ, asc, nullLast))
              case Some((columnLabel, NamedExpr(existingExpr, _, _, _))) =>
                (None, (columnLabel, existingExpr.typ, asc, nullLast))
            }
          }.unzip

          val newSelf = orderedSelf.copy(selectList = selectList ++ newColumns.flatten)

          (outputInfo, newSelf)
        } else {
          // Caller doesn't care what we're ordered by, so no need
          // to select anything to accomodate them.
          (Nil, orderedSelf)
        }
    }
  }

  private def rewriteFrom(availableCTEs: ACTEs, from: From, wantOutputOrdered: Boolean, wantOrderingColumns: Boolean): (Seq[OrderBy], From) = {
    from match {
      case join: Join =>
        // JOIN builds a new table, which is unordered (hence (false,
        // false) and why this entire rewriteFrom method isn't just a
        // call to from.map
        val result = join.map[MT](
          rewriteAtomicFrom(availableCTEs, _, false, false)._2,
          { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(availableCTEs, right, false, false)._2, on) }
        )
        (Nil, result)
      case af: AtomicFrom =>
        rewriteAtomicFrom(availableCTEs, af, wantOutputOrdered, wantOrderingColumns)
    }
  }

  private def rewriteAtomicFrom(availableCTEs: ACTEs, from: AtomicFrom, wantOutputOrdered: Boolean, wantOrderingColumns: Boolean): (Seq[OrderBy], AtomicFrom) = {
    from match {
      case ft: FromTable => (Nil, ft)
      case fs: FromSingleRow => (Nil, fs)
      case fc: FromCTE =>
        availableCTEs.ctes.get(fc.cteLabel) match {
          case Some(cteInfo) =>
            val newCTE = availableCTEs.rebase(fc)
            val orderingColumns =
              if(wantOrderingColumns) {
                cteInfo.extra.map { case (col, typ, asc, nullLast) => OrderBy(VirtualColumn[MT](newCTE.label, col, typ)(AtomicPositionInfo.Synthetic), asc, nullLast) }
              } else {
                Nil
              }
            (orderingColumns, newCTE)
          case None =>
            // This shouldn't actually happen, but if it does, we just
            // didn't rewrite anything.  If it does happen, it means
            // the rewrite pass was applied to something that wasn't a
            // top-level statement, and it references a CTE defined in
            // the higher level to which the pass has no access.
            (Nil, fc)
        }
      case fs@FromStatement(stmt, label, resourceName, canonicalName, alias) =>
        val (orderColumn, newStmt) = rewriteStatement(availableCTEs, stmt, wantOutputOrdered, wantOrderingColumns)
        (orderColumn.map { case (col, typ, asc, nullLast) => OrderBy(VirtualColumn(label, col, typ)(AtomicPositionInfo.Synthetic), asc, nullLast) }, fs.copy(statement = newStmt))
    }
  }
}

/** Attempt to preserve ordering from inner queries to outer ones.
  * SelectListReferences must not be present (this is unchecked!!). */
object PreserveOrdering {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new PreserveOrdering[MT](labelProvider).rewriteStatement(AvailableCTEs.empty, stmt, true, false)._2
  }

  def withExtraOutputColumns[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new PreserveOrdering[MT](labelProvider).rewriteStatement(AvailableCTEs.empty, stmt, true, true)._2
  }
}
