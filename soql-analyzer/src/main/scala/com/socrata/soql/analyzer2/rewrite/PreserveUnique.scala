package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._
import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.MonomorphicFunction

class PreserveUnique[MT <: MetaTypes] private (provider: LabelProvider) extends StatementUniverse[MT] {
  def rewriteStatement(stmt: Statement, wantColumns: Boolean): Statement = {
    stmt match {
      case ct@CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        ct

      case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        val newUseQuery = rewriteStatement(useQuery, wantColumns)

        cte.copy(
          useQuery = newUseQuery
        )

      case v@Values(_, _) =>
        // TODO, should this rewrite into a SELECT ... FROM
        // (values...) ORDER BY synthetic_column ?  If so, we'll need
        // a way to generate synthetic_column (== have a way to turn
        // an integer into a CV).  See also similar comment over in
        // ImposeOrdering.
        v

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

        if(distinctiveness == Distinctiveness.FullyDistinct()) {
          select
        } else {
          val newFrom = rewriteFrom(from)
          if(wantColumns) {
            var existingExprs = selectList.valuesIterator.map(_.expr).to(Set)
            val additional = Vector.newBuilder[Column]
            for(col <- newFrom.unique.flatten) {
              if(!existingExprs(col)) {
                existingExprs += col
                additional += col
              }
            }
            val newColumns = additional.result().map { col =>
              provider.columnLabel() -> NamedExpr(col, freshName("unique"), isSynthetic = true)
            }
            select.copy(selectList = selectList ++ newColumns, from = newFrom)
          } else {
            select.copy(from = newFrom)
          }
        }
    }
  }

  def rewriteFrom(from: From): From = {
    from.map[MT](
      rewriteAtomicFrom,
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right), on) }
    )
  }

  def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
    from match {
      case ft: FromTable => ft
      case fs: FromSingleRow => fs
      case fs@FromStatement(stmt, label, resourceName, alias) =>
        val newStmt = rewriteStatement(stmt, true)
        fs.copy(statement = newStmt)
    }
  }
}

/** Attempt to preserve ordering from inner queries to outer ones.
  * SelectListReferences must not be present (this is unchecked!!). */
object PreserveUnique {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new PreserveUnique[MT](labelProvider).rewriteStatement(stmt, false)
  }
}
