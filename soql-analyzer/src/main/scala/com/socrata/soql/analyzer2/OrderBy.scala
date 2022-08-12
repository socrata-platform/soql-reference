package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.typechecker.HasDoc

case class OrderBy[+CT, +CV](expr: Expr[CT, CV], ascending: Boolean, nullLast: Boolean) {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))

  def debugDoc(implicit ev: HasDoc[CV]) =
    Seq(
      expr.debugDoc,
      Seq(
        if(ascending) d"ASC" else d"DESC",
        if(nullLast) d"NULLS LAST" else d"NULLS FIRST"
      ).hsep
    ).sep.nest(2)
}
