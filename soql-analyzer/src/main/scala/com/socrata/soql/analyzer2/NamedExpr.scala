package com.socrata.soql.analyzer2

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.analyzer2.serialization.{Writable, WriteBuffer}

case class NamedExpr[+CT, +CV](expr: Expr[CT, CV], name: ColumnName) {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))
}

object NamedExpr {
  implicit def serialize[CT: Writable, CV: Writable] = new Writable[NamedExpr[CT, CV]] {
    def writeTo(buffer: WriteBuffer, ne: NamedExpr[CT, CV]): Unit = {
      buffer.write(ne.expr)
      buffer.write(ne.name)
    }
  }
}
