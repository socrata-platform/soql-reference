package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveTrivialSelects[MT <: MetaTypes] private () extends StatementUniverse[MT] {
  // A "trivial select" is a select of the form:
  //   select column_ref, ... from (non-output-schema-sensitive subselect)
  // without any other modifiers and where all the column_refs point
  // into the from.
  //
  // A select is output-schema-sensitive if it contains a DISTINCT or
  // a SEARCH (n.b., currently SEARCH is implemented on input data
  // rather than on output data, but I'm still going to pretend that
  // it might someday change to the more useful semantics)

  // This is mainly a sample rewrite pass; `merge` does a lot more.

  private def isTrivialExpr(e: Expr) =
    e match {
      case _ : Column => true
      case _ : Literal => true
      case _ => false
    }

  // Invariant: the resulting statement will have the same output
  // schema as the input statement.  The implications of this are:
  //   1. Easier to reason about (no need to track schema changes
  //      to rewrite references to replaced columns).
  //   2. Selects will only be combined with other selects, not
  //      tables (even if doing so could be semantics-preserving)
  //      because a table's columns have DatabaseTableNames and
  //      a select's columns have AutoColumnLabels.
  def rewriteStatement(stmt: Statement): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left), rewriteStatement(right))

      case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        val newDefQuery = rewriteStatement(defQuery)
        val newUseQuery = rewriteStatement(useQuery)
        CTE(defLabel, defAlias, newDefQuery, materializedHint, newUseQuery)

      case v@Values(_, _) =>
        v

      case originalSel@Select(Distinctiveness.Indistinct(), originalSelectList, from, None, Nil, None, Nil, None, None, None, hints)
          if hints.isEmpty && originalSelectList.values.forall { se => isTrivialExpr(se.expr) }
          =>
        rewriteFrom(from) match {
          case FromStatement(
            newSel@Select(Distinctiveness.Indistinct() | Distinctiveness.On(_), newSelectList, _from, _where, _groupBy, _having, _orderBy, _limit, _offset, None, _hint),
            label, _resourceName, _canonicalName, _alias
          ) =>
            newSel.copy(
              selectList = originalSelectList.withValuesMapped { se =>
                se.expr match {
                  case VirtualColumn(table, column, _typ) if table == label =>
                    val parentSE = newSelectList(column)
                    NamedExpr(
                      parentSE.expr,
                      se.name,
                      se.hint orElse parentSE.hint,
                      se.isSynthetic
                    )
                  case _ : Column =>
                    // brought in from a sibling table via lateral join
                    se
                  case _ : Literal =>
                    se
                  case _ =>
                    throw new Exception("All exprs were trivial before???")
                }
              }
            )

          case other =>
            originalSel.copy(from = other)
        }

      case sel: Select =>
        sel.copy(from = rewriteFrom(sel.from))
    }
  }

  def rewriteFrom(from: From): From = {
    from.map[MT](
      rewriteAtomicFrom,
      { (joinType, lateral, left, right, on) =>
        val newRight = rewriteAtomicFrom(right)
        Join(joinType, lateral, left, newRight, on)
      }
    )
  }

  def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
    from match {
      case ft: FromTable => ft
      case fsr: FromSingleRow => fsr
      case fc: FromCTE => fc
      case fs@FromStatement(stmt, _label, _resourceName, _canonicalName, _alias) =>
        fs.copy(statement = rewriteStatement(stmt))
    }
  }
}

/** Remove trivial selects, where "trivial" means "only selects
  * columns from some single subselect, with no other action". */
object RemoveTrivialSelects {
  def apply[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    new RemoveTrivialSelects[MT]().rewriteStatement(stmt)
  }
}
