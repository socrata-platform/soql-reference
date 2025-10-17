package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._
import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.MonomorphicFunction

class PreserveUnique[MT <: MetaTypes] private (provider: LabelProvider) extends StatementUniverse[MT] {
  type ACTEs = AvailableCTEs[MT, Unit]

  def rewriteStatement(availableCTEs: ACTEs, stmt: Statement, wantColumns: Boolean): Statement = {
    stmt match {
      case ct@CombinedTables(op, left, right) =>
        // combined tables cannot guarantee unique columns exist.
        // More specifically, we cannot change left & right in a way
        // that passes through unselected unique columns without
        // potentially changing the results of the query.
        ct

      case cte@CTE(defns, useQuery) =>
        val (newAvailableCTEs, newDefinitions) = availableCTEs.collect(defns) { (aCTEs, query) =>
          ((), rewriteStatement(aCTEs, query, true))
        }

        val newUseQuery = rewriteStatement(newAvailableCTEs, useQuery, wantColumns)

        CTE(newDefinitions, newUseQuery)

      case v@Values(_, _) =>
        // valueses don't have unique columns
        v

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        if(distinctiveness == Distinctiveness.FullyDistinct()) {
          // Can't add columns, so we just need to accept what we're given
          select
        } else {
          val usedNames = selectList.valuesIterator.map(_.name).to(scm.HashSet)
          def freshName(base: String) = {
            val created = Iterator.from(1).map { i => ColumnName(base + "_" + i) }.find { n =>
              !usedNames.contains(n)
            }.get
            usedNames += created
            created
          }

          // pull up un-selected unique columns from the FROM clause
          // if we're un-aggregated, otherwise pull up un-selected
          // columns from our GROUP BY clause
          val newFrom =
            if(select.isAggregated) availableCTEs.rebaseAll(from) // not touching the rest but need to rebase CTEs anyway
            else rewriteFrom(availableCTEs, from)

          if(wantColumns) {
            var existingExprs = selectList.valuesIterator.map(_.expr).to(Set)
            val additional = Vector.newBuilder[Expr]
            val newColumnSource =
              if(select.isAggregated) groupBy
              else newFrom.unique.flatten

            for(col <- newColumnSource) {
              if(!existingExprs(col)) {
                existingExprs += col
                additional += col
              }
            }
            val newColumns = additional.result().map { col =>
              provider.columnLabel() -> NamedExpr(col, freshName("unique"), hint = None, isSynthetic = true)
            }
            select.copy(selectList = selectList ++ newColumns, from = newFrom)
          } else {
            select.copy(from = newFrom)
          }
        }
    }
  }

  def rewriteFrom(availableCTEs: ACTEs, from: From): From = {
    from.map[MT](
      rewriteAtomicFrom(availableCTEs, _),
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(availableCTEs, right), on) }
    )
  }

  def rewriteAtomicFrom(availableCTEs: ACTEs, from: AtomicFrom): AtomicFrom = {
    from match {
      case ft: FromTable => ft
      case fs: FromSingleRow => fs
      case fc: FromCTE => availableCTEs.rebase(fc)
      case fs@FromStatement(stmt, label, resourceName, canonicalName, alias) =>
        val newStmt = rewriteStatement(availableCTEs, stmt, true)
        fs.copy(statement = newStmt)
    }
  }
}

/** Attempt to preserve ordering from inner queries to outer ones.
  * SelectListReferences must not be present (this is unchecked!!). */
object PreserveUnique {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new PreserveUnique[MT](labelProvider).rewriteStatement(AvailableCTEs.empty, stmt, false)
  }
}
