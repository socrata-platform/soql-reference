package com.socrata.soql.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.NonEmptySeq

trait SplitAnd[MT <: MetaTypes] extends StatementUniverse[MT] {
  def split(e: Expr): NonEmptySeq[Expr]
  def merge(es: NonEmptySeq[Expr]): Expr
}
