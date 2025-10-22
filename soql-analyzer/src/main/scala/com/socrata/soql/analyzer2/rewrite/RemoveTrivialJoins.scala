package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveTrivialJoins[MT <: MetaTypes] private (isLiteralTrue: Expr[MT] => Boolean) extends StatementUniverse[MT] {
  type ACTEs = AvailableCTEs[MT, Unit]

  def rewriteStatement(availableCTEs: ACTEs, stmt: Statement): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(availableCTEs, left), rewriteStatement(availableCTEs, right))

      case CTE(defns, useQuery) =>
        val (newAvailableCTEs, newDefns) = availableCTEs.collect(defns) { (aCTEs, query) =>
          ((), rewriteStatement(aCTEs, query))
        }
        val newUseQuery = rewriteStatement(newAvailableCTEs, useQuery)
        CTE(newDefns, newUseQuery)

      case v@Values(_, _) =>
        v

      case sel: Select =>
        sel.copy(from = rewriteFrom(availableCTEs, sel.from))
    }
  }

  def rewriteFrom(availableCTEs: ACTEs, from: From): From = {
    from match {
      case Join(JoinType.Inner, _, left, FromSingleRow(_, _), on) if isLiteralTrue(on) =>
        rewriteFrom(availableCTEs, left)
      case Join(JoinType.Inner, _, FromSingleRow(_, _), right, on) if isLiteralTrue(on) =>
        rewriteAtomicFrom(availableCTEs, right)
      case j: Join =>
        j.copy(left = rewriteFrom(availableCTEs, j.left), right = rewriteAtomicFrom(availableCTEs, j.right))
      case af: AtomicFrom =>
        rewriteAtomicFrom(availableCTEs, af)
    }
  }

  def rewriteAtomicFrom(availableCTEs: ACTEs, from: AtomicFrom): AtomicFrom = {
    from match {
      case fs: FromStatement => fs.copy(statement = rewriteStatement(availableCTEs, fs.statement))
      case fc: FromCTE => availableCTEs.rebase(fc)
      case nonStatement => nonStatement
    }
  }
}

/** Remove trivial selects, where "trivial" means "only selects
  * columns from some single subselect, with no other action". */
object RemoveTrivialJoins {
  def apply[MT <: MetaTypes](stmt: Statement[MT], isLiteralTrue: Expr[MT] => Boolean): Statement[MT] = {
    new RemoveTrivialJoins[MT](isLiteralTrue).rewriteStatement(AvailableCTEs.empty, stmt)
  }
}
