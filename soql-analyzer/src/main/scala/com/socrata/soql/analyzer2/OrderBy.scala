package com.socrata.soql.analyzer2

case class OrderBy[+CT, +CV](expr: Expr[CT, CV], ascending: Boolean, nullLast: Boolean) {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))

  def debugStr(sb: StringBuilder): StringBuilder = {
    expr.debugStr(sb)
    if(ascending) {
      sb.append(" ASC")
    } else {
      sb.append(" DESC")
    }
    if(nullLast) {
      sb.append(" NULLS LAST")
    } else {
      sb.append(" NULLS FIRST")
    }
  }
}
