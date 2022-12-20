package com.socrata.soql.analyzer2.rewrite

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.MonomorphicFunction

class PreserveOrdering[RNS, CT, CV] private (provider: LabelProvider) {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type Select = analyzer2.Select[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]
  type OrderBy = analyzer2.OrderBy[CT, CV]

  // "wantOutputOrdered" == "if this statement can be rewritten to
  // preserve the ordering of its underlying query, do so".
  // "wantOrderingColumns" == "The caller needs column from this table
  // to order itself".  Note that just because the caller wants a
  // thing, it will not necessarily get it!
  def rewriteStatement(stmt: Statement, wantOutputOrdered: Boolean, wantOrderingColumns: Boolean): (Seq[(ColumnLabel, CT, Boolean, Boolean)], Statement) = {
    stmt match {
      case CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        (Nil, CombinedTables(op, rewriteStatement(left, false, false)._2, rewriteStatement(right, false, false)._2))

      case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        val (orderingColumns, newUseQuery) = rewriteStatement(useQuery, wantOutputOrdered, wantOrderingColumns)

        (
          orderingColumns,
          cte.copy(
            definitionQuery = rewriteStatement(defQuery, true, false)._2,
            useQuery = newUseQuery
          )
        )

      case v@Values(_) =>
        // TODO, should this rewrite into a SELECT ... FROM
        // (values...) ORDER BY synthetic_column ?  If so, we'll need
        // a way to generate synthetic_column (== have a way to turn
        // an integer into a CV)
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

        // If we're windowed, we want the underlying query ordered if
        // possible even if our caller doesn't care, unless there's an
        // aggregate in the way, in which case the aggregate will
        // destroy any underlying ordering anyway so we stop caring.

        val wantSubqueryOrdered = (select.isWindowed || wantOutputOrdered) && !select.isAggregated && distinctiveness == Distinctiveness.Indistinct
        val (extraOrdering, newFrom) = rewriteFrom(from, wantSubqueryOrdered, wantSubqueryOrdered)

        // We will at the very least want to add the ordering
        // columns from the subquery onto the end of our order-by...
        val orderedSelf = select.copy(
          from = newFrom,
          orderBy = orderBy ++ extraOrdering
        )
        if(wantOrderingColumns) {
          // ..and our caller wants to know how we're ordered, so
          // make sure we've put the relevant expressins in our
          // select list and return them.
          val (newColumns, outputInfo) = orderedSelf.orderBy.map { case OrderBy(expr, asc, nullLast) =>
            val columnLabel = provider.columnLabel()
            (columnLabel -> NamedExpr(expr, freshName("order")), (columnLabel, expr.typ, asc, nullLast))
          }.unzip

          val newSelf = orderedSelf.copy(selectList = selectList ++ newColumns)

          (outputInfo, newSelf)
        } else {
          // Caller doesn't care what we're ordered by, so no need
          // to select anything to accomodate them.
          (Nil, orderedSelf)
        }
    }
  }

  def rewriteFrom(from: From, wantOutputOrdered: Boolean, wantOrderingColumns: Boolean): (Seq[OrderBy], From) = {
    from match {
      case join: Join =>
        // JOIN builds a new table, which is unordered (hence (false,
        // false) and why this entire rewriteFrom method isn't just a
        // call to from.map
        val result = join.map[RNS, CT, CV](
          rewriteAtomicFrom(_, false, false)._2,
          { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right, false, false)._2, on) }
        )
        (Nil, result)
      case af: AtomicFrom =>
        rewriteAtomicFrom(af, wantOutputOrdered, wantOrderingColumns)
    }
  }

  def rewriteAtomicFrom(from: AtomicFrom, wantOutputOrdered: Boolean, wantOrderingColumns: Boolean): (Seq[OrderBy], AtomicFrom) = {
    from match {
      case ft: FromTable => (Nil, ft)
      case fs: FromSingleRow => (Nil, fs)
      case fs@FromStatement(stmt, label, resourceName, alias) =>
        val (orderColumn, newStmt) = rewriteStatement(stmt, wantOutputOrdered, wantOrderingColumns)
        (orderColumn.map { case (col, typ, asc, nullLast) => OrderBy(Column(label, col, typ)(AtomicPositionInfo.None), asc, nullLast) }, fs.copy(statement = newStmt))
    }
  }
}

object PreserveOrdering {
  def apply[RNS, CT, CV](labelProvider: LabelProvider, stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    new PreserveOrdering[RNS, CT, CV](labelProvider).rewriteStatement(stmt, true, false)._2
  }
}
