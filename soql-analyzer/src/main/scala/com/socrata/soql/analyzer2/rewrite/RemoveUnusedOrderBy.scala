package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class RemoveUnusedOrderBy[MT <: MetaTypes] private () extends StatementUniverse[MT] {
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

      case v@Values(_, _) =>
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
    from.map[MT](
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
  def apply[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    new RemoveUnusedOrderBy[MT]().rewriteStatement(stmt, true)
  }
}
