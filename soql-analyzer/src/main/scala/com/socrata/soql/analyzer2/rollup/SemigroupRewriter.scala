package com.socrata.soql.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

trait SemigroupRewriter[MT <: MetaTypes]
    extends (MonomorphicFunction[MT#ColumnType] => Option[Expr[MT] => Expr[MT]])
    with StatementUniverse[MT]
{
  // This is responsible for doing things that look like
  //   count(x) => coalesce(sum(count_x), 0)
  //   max(x) => max(max_x)
  override def apply(f: MonomorphicFunction): Option[Expr => Expr]

  // tests whether an aggregate function is a semilattice (which for
  // our purposes means that f(f(x)) == f(x), which in turn means we
  // can rewrite `f(x)` by using `select x group by x`, since we don't
  // care that each row of "x" might actually consist of multiple
  // source rows.
  def isSemilattice(f: MonomorphicFunction): Boolean
}
