package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2._

class RemoveSyntheticColumns[MT <: MetaTypes] private (labelProvider: LabelProvider) extends RemoveOutputColumns[MT](labelProvider, _.isSynthetic)

object RemoveSyntheticColumns {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new RemoveSyntheticColumns[MT](labelProvider).rewriteStatement(stmt)
  }
}
