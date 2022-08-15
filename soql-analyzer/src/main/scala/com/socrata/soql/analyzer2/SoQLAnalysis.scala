package com.socrata.soql.analyzer2

import com.socrata.soql.functions.MonomorphicFunction

class SoQLAnalysis[RNS, CT, CV] private[analyzer2] (
  val labelProvider: LabelProvider,
  val statement: Statement[RNS, CT, CV]
) {
  /** Rewrite the analysis plumbing through enough information to
    * preserve table-ordering (except across joins and aggregates,
    * which of course destroy ordering). */
  def preserveOrdering(rowNumberFunction: MonomorphicFunction[CT]): SoQLAnalysis[RNS, CT, CV] =
    copy(statement = statement.preserveOrdering(labelProvider, rowNumberFunction, true, false)._2)

  /** Simplify subselects on a best-effort basis. */
  def merge: SoQLAnalysis[RNS, CT, CV] =
    copy(statement = this.statement)

  /** Rewrite expressions in group/order/distinct clauses which are
    * identical to expressions in the select list to use select-list
    * indexes instead. */
  def useSelectListReferences =
    copy(statement = statement.useSelectListReferences)

  private def copy[RNS2, CT2, CV2](
    labelProvider: LabelProvider = this.labelProvider,
    statement: Statement[RNS2, CT2, CV2] = this.statement
  ) =
    new SoQLAnalysis(labelProvider, statement)
}
