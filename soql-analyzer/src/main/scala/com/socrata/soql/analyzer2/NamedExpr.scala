package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer, Version}

case class NamedExpr[MT <: MetaTypes](expr: Expr[MT], name: ColumnName, hint: Option[JValue], isSynthetic: Boolean) extends LabelUniverse[MT] {
  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))

  def typ = expr.typ
}

object NamedExpr {
  implicit def serialize[MT <: MetaTypes](implicit ev: Writable[Expr[MT]]) = new Writable[NamedExpr[MT]] {
    def writeTo(buffer: WriteBuffer, ne: NamedExpr[MT]): Unit = {
      buffer.write(ne.expr)
      buffer.write(ne.name)
      buffer.write(ne.isSynthetic)
      buffer.write(ne.hint)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit ev: Readable[Expr[MT]]) = new Readable[NamedExpr[MT]] {
    def readFrom(buffer: ReadBuffer): NamedExpr[MT] = {
      buffer.version match {
        case Version.V0 | Version.V1 =>
          NamedExpr(
            expr = buffer.read[Expr[MT]](),
            name = buffer.read[ColumnName](),
            isSynthetic = buffer.read[Boolean](),
            hint = None
          )
        case Version.V2 =>
          NamedExpr(
            expr = buffer.read[Expr[MT]](),
            name = buffer.read[ColumnName](),
            isSynthetic = buffer.read[Boolean](),
            hint = buffer.read[Option[JValue]]()
          )
      }
    }
  }
}
