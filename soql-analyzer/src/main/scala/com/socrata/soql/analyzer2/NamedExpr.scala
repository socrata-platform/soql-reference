package com.socrata.soql.analyzer2

import com.socrata.soql.environment.ColumnName

case class NamedExpr[+CT, +CV](expr: Expr[CT, CV], name: ColumnName) {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))
}
