package com.socrata.soql.analyzer2.rollup

import com.socrata.soql.analyzer2._

trait AdHocRewriter[MT <: MetaTypes] extends StatementUniverse[MT] {
  // Rewrite a given expression into alternate candidate expressions.
  // This is called if the rewriter hasn't seen any other way to
  // handle mapping an expression onto a rollup.
  def apply(e: Expr): Seq[Expr]
}

object AdHocRewriter {
  def noop[MT <: MetaTypes] = new AdHocRewriter[MT] {
    override def apply(e: Expr) = Nil
  }
}
