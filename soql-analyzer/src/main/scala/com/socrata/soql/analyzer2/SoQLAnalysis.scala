package com.socrata.soql.analyzer2

import com.socrata.soql.functions.MonomorphicFunction

class SoQLAnalysis[RNS, CT, CV] private (
  val labelProvider: LabelProvider,
  val statement: Statement[RNS, CT, CV],
  usesSelectListReferences: Boolean
) {
  private[analyzer2] def this(labelProvider: LabelProvider, statement: Statement[RNS, CT, CV]) =
    this(labelProvider, statement, false)

  /** Rewrite the analysis plumbing through enough information to
    * preserve table-ordering (except across joins and aggregates,
    * which of course destroy ordering). */
  def preserveOrdering(rowNumberFunction: MonomorphicFunction[CT]): SoQLAnalysis[RNS, CT, CV] =
    copy(statement = statement.preserveOrdering(labelProvider, rowNumberFunction, true, false)._2)

  /** Simplify subselects on a best-effort basis. */
  def merge(and: MonomorphicFunction[CT]): SoQLAnalysis[RNS, CT, CV] =
    copy(statement = new Merger(and).merge(statement))

  /** Simplify subselects on a best-effort basis. */
  def removeUnusedColumns: SoQLAnalysis[RNS, CT, CV] =
    if(usesSelectListReferences) copy(statement = statement.unuseSelectListReferences.removeUnusedColumns.useSelectListReferences)
    else copy(statement = statement.removeUnusedColumns)

  /** Rewrite expressions in group/order/distinct clauses which are
    * identical to expressions in the select list to use select-list
    * indexes instead. */
  def useSelectListReferences =
    if(usesSelectListReferences) this
    else copy(statement = statement.useSelectListReferences, usesSelectListReferences = true)

  private def copy[RNS2, CT2, CV2](
    labelProvider: LabelProvider = this.labelProvider,
    statement: Statement[RNS2, CT2, CV2] = this.statement,
    usesSelectListReferences: Boolean = this.usesSelectListReferences
  ) =
    new SoQLAnalysis(labelProvider, statement, usesSelectListReferences)
}
