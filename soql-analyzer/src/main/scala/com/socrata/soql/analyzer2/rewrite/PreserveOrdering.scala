package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.MonomorphicFunction

class PreserveOrdering[RNS, CT, CV] private (provider: LabelProvider, rowNumberFunction: MonomorphicFunction[CT]) {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type Select = analyzer2.Select[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]

  def rewriteStatement(stmt: Statement, wantOutputOrdered: Boolean, wantOrderingColumn: Boolean): (Option[AutoColumnLabel], Statement) = {
    stmt match {
      case CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        (None, CombinedTables(op, rewriteStatement(left, false, false)._2, rewriteStatement(right, false, false)._2))

      case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        val (orderingColumn, newUseQuery) = rewriteStatement(useQuery, wantOutputOrdered, wantOrderingColumn)

        (
          orderingColumn,
          cte.copy(
            definitionQuery = rewriteStatement(defQuery, false, false)._2,
            useQuery = newUseQuery
          )
        )

      case v@Values(_) =>
        // TODO, should this rewrite into a SELECT ... FROM (values...) ORDER BY synthetic_column ?
        // If so, we'll need a way to generate synthetic_column
        (None, v)

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        // If we're windowed, we want the underlying query ordered if
        // possible even if our caller doesn't care, unless there's an
        // aggregate in the way, in which case the aggregate will
        // destroy any underlying ordering anyway so we stop caring.

        def freshName(base: String) = {
          val names = selectList.valuesIterator.map(_.name).toSet
          Iterator.from(1).map { i => ColumnName(base + "_" + i) }.find { n =>
            !names.contains(n)
          }.get
        }

        val wantSubqueryOrdered = (select.isWindowed || wantOutputOrdered) && !select.isAggregated && distinctiveness == Distinctiveness.Indistinct
        rewriteFrom(from, wantSubqueryOrdered, wantSubqueryOrdered) match {
          case (Some((table, column)), newFrom) =>
            val col = Column(table, column, rowNumberFunction.result)(AtomicPositionInfo.None)

            val orderedSelf = select.copy(
              from = newFrom,
              orderBy = orderBy :+ OrderBy(col, true, true)
            )

            if(wantOrderingColumn) {
              val rowNumberLabel = provider.columnLabel()
              val newSelf = orderedSelf.copy(
                selectList = selectList + (rowNumberLabel -> (NamedExpr(col, freshName("order"))))
              )

              (Some(rowNumberLabel), newSelf)
            } else {
              (None, orderedSelf)
            }

          case (None, newFrom) =>
            if(wantOrderingColumn && orderBy.nonEmpty) {
              // assume the given order by provides a total order and
              // reflect that in our ordering column

              val rowNumberLabel = provider.columnLabel()
              val newSelf = select.copy(
                selectList = selectList + (rowNumberLabel -> NamedExpr(WindowedFunctionCall(rowNumberFunction, Nil, None, Nil, Nil, None)(FuncallPositionInfo.None), freshName("order"))),
                from = newFrom
              )

              (Some(rowNumberLabel), newSelf)
            } else {
              // No ordered FROM _and_ no ORDER BY?  You don't get a column even though you asked for one
              (None, select.copy(from = newFrom))
            }
        }
    }
  }

  def rewriteFrom(from: From, wantOutputOrdered: Boolean, wantOrderingColumn: Boolean): (Option[(TableLabel, AutoColumnLabel)], From) = {
    from match {
      case join: Join =>
        // JOIN builds a new table, which is unordered (hence (false,
        // false) and why this entire rewriteFrom method isn't just a
        // call to from.map
        val result = join.map[RNS, CT, CV](
          rewriteAtomicFrom(_, false, false)._2,
          { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right, false, false)._2, on) }
        )
        (None, result)
      case af: AtomicFrom =>
        rewriteAtomicFrom(af, wantOutputOrdered, wantOrderingColumn)
    }
  }

  def rewriteAtomicFrom(from: AtomicFrom, wantOutputOrdered: Boolean, wantOrderingColumn: Boolean): (Option[(TableLabel, AutoColumnLabel)], AtomicFrom) = {
    from match {
      case ft: FromTable => (None, ft)
      case fs: FromSingleRow => (None, fs)
      case fs@FromStatement(stmt, label, resourceName, alias) =>
        val (orderColumn, newStmt) = rewriteStatement(stmt, wantOutputOrdered, wantOrderingColumn)
        (orderColumn.map((label, _)), fs.copy(statement = newStmt))
    }
  }
}

object PreserveOrdering {
  def apply[RNS, CT, CV](labelProvider: LabelProvider, rowNumberFunction: MonomorphicFunction[CT], stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    new PreserveOrdering[RNS, CT, CV](labelProvider, rowNumberFunction).rewriteStatement(stmt, true, false)._2
  }
}
