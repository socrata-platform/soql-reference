package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class RemoveUnusedOrderBy[RNS, CT, CV] private () {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type Select = analyzer2.Select[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]
  type OrderBy = analyzer2.OrderBy[CT, CV]

  def rewriteStatement(stmt: Statement, callerCaresAboutOrder: Boolean): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        CombinedTables(op, rewriteStatement(left, false), rewriteStatement(right, false))

      case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        val newUseQuery = rewriteStatement(useQuery, callerCaresAboutOrder)

        cte.copy(
          definitionQuery = rewriteStatement(defQuery, false),
          useQuery = newUseQuery
        )

      case v@Values(_) =>
        v

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val newFrom = rewriteFrom(from)

        select.copy(
          from = newFrom,
          orderBy = if(callerCaresAboutOrder || limit.isDefined || offset.isDefined || select.isWindowed) orderBy else Nil
        )
    }
  }

  def rewriteFrom(from: From): From = {
    from.map[RNS, CT, CV](
      rewriteAtomicFrom(_),
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right), on) }
    )
  }

  def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
    from match {
      case ft: FromTable => ft
      case fs: FromSingleRow => fs
      case fs@FromStatement(stmt, label, resourceName, alias) =>
        fs.copy(statement = rewriteStatement(stmt, false))
    }
  }
}

/** Attempt to preserve ordering from inner queries to outer ones.
  * SelectListReferences must not be present (this is unchecked!!). */
object RemoveUnusedOrderBy {
  def apply[RNS, CT, CV](stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    new RemoveUnusedOrderBy[RNS, CT, CV]().rewriteStatement(stmt, true)
  }
}
