package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveUnusedOrderBy[MT <: MetaTypes] private () extends StatementUniverse[MT] {
  type ACTEs = AvailableCTEs[Unit]

  case class Result(statement: Statement)

  def rewriteStatement(availableCTEs: ACTEs, stmt: Statement, callerCaresAboutOrder: Boolean): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        CombinedTables(op, rewriteStatement(availableCTEs, left, false), rewriteStatement(availableCTEs, right, false))

      case CTE(defns, useQuery) =>
        val (newAvailableCTEs, newDefinitions) = availableCTEs.collect(defns) { (aCTEs, query) =>
          ((), rewriteStatement(aCTEs, query, false))
        }

        val newUseQuery = rewriteStatement(newAvailableCTEs, useQuery, callerCaresAboutOrder)

        CTE(newDefinitions, newUseQuery)

      case v@Values(_, _) =>
        v

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val newFrom = rewriteFrom(availableCTEs, from)

        select.copy(
          from = newFrom,
          orderBy = if(callerCaresAboutOrder || select.isWindowed || limit.isDefined || offset.isDefined) orderBy else Nil
        )
    }
  }

  def rewriteFrom(availableCTEs: ACTEs, from: From): From = {
    from.map[MT](
      rewriteAtomicFrom(availableCTEs, _),
      { (joinType, lateral, left, right, on) =>
        val newRight = rewriteAtomicFrom(availableCTEs, right)
        Join(joinType, lateral, left, newRight, on)
      }
    )
  }

  def rewriteAtomicFrom(availableCTEs: ACTEs, from: AtomicFrom): AtomicFrom = {
    from match {
      case ft: FromTable => ft
      case fs: FromSingleRow => fs
      case fc: FromCTE => availableCTEs.rebase(fc)
      case fs@FromStatement(stmt, label, resourceName, canonicalName, alias) =>
        // It's possible we are reducing a statement to "select
        // column1, column2, ... from table" and we want to eliminate
        // the now-unnecessary subselect.  But we won't, because that
        // interacts in surprising ways with SEARCH.  Let a subsequent
        // merge pass (which knows about that kind of thing) get rid
        // of it if we want it to go away.
        fs.copy(statement = rewriteStatement(availableCTEs, stmt, false))
    }
  }
}

/** Attempt to preserve ordering from inner queries to outer ones.
  * SelectListReferences must not be present (this is unchecked!!). */
object RemoveUnusedOrderBy {
  def apply[MT <: MetaTypes](stmt: Statement[MT], preserveTopLevelOrdering: Boolean = true): Statement[MT] = {
    new RemoveUnusedOrderBy[MT]().rewriteStatement(AvailableCTEs.empty, stmt, preserveTopLevelOrdering)
  }
}
