package com.socrata.soql.analyzer2

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

case class NamedExpr[+CT, +CV](expr: Expr[CT, CV], name: ColumnName) {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))
}

object NamedExpr {
  implicit def serialize[CT, CV](implicit ev: Writable[Expr[CT, CV]]) = new Writable[NamedExpr[CT, CV]] {
    def writeTo(buffer: WriteBuffer, ne: NamedExpr[CT, CV]): Unit = {
      buffer.write(ne.expr)
      buffer.write(ne.name)
    }
  }

  implicit def deserialize[CT, CV](implicit ev: Readable[Expr[CT, CV]]) = new Readable[NamedExpr[CT, CV]] {
    def readFrom(buffer: ReadBuffer): NamedExpr[CT, CV] = {
      NamedExpr(
        expr = buffer.read[Expr[CT, CV]](),
        name = buffer.read[ColumnName]()
      )
    }
  }
}