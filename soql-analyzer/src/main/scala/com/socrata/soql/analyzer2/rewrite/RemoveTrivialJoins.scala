package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveTrivialJoins[MT <: MetaTypes] private (isLiteralTrue: Expr[MT] => Boolean) extends RewritePassMixin[MT] {
  def rewriteStatement(stmt: Statement): Statement = {
    rewriteExpressionSubqueries(stmt, rewriteStatement) match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left), rewriteStatement(right))

      case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        val newDefQuery = rewriteStatement(defQuery)
        val newUseQuery = rewriteStatement(useQuery)
        CTE(defLabel, defAlias, newDefQuery, materializedHint, newUseQuery)

      case v@Values(_, _) =>
        v

      case sel: Select =>
        sel.copy(from = rewriteFrom(sel.from))
    }
  }

  def rewriteFrom(from: From): From = {
    from match {
      case Join(JoinType.Inner, _, left, FromSingleRow(_, _), on) if isLiteralTrue(on) =>
        rewriteFrom(left)
      case Join(JoinType.Inner, _, FromSingleRow(_, _), right, on) if isLiteralTrue(on) =>
        rewriteAtomicFrom(right)
      case j: Join =>
        j.copy(left = rewriteFrom(j.left), right = rewriteAtomicFrom(j.right))
      case af: AtomicFrom =>
        rewriteAtomicFrom(af)
    }
  }

  def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
    from match {
      case fs: FromStatement => fs.copy(statement = rewriteStatement(fs.statement))
      case nonStatement => nonStatement
    }
  }
}

/** Remove trivial selects, where "trivial" means "only selects
  * columns from some single subselect, with no other action". */
object RemoveTrivialJoins {
  def apply[MT <: MetaTypes](stmt: Statement[MT], isLiteralTrue: Expr[MT] => Boolean): Statement[MT] = {
    new RemoveTrivialJoins[MT](isLiteralTrue).rewriteStatement(stmt)
  }
}
