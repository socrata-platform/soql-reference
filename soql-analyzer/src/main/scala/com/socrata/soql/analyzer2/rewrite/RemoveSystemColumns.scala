package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2._

class RemoveSystemColumns[MT <: MetaTypes] private (labelProvider: LabelProvider) extends RemoveOutputColumns[MT](labelProvider, _.name.isSystem)

object RemoveSystemColumns {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new RemoveSystemColumns[MT](labelProvider).rewriteStatement(stmt)
  }
}
