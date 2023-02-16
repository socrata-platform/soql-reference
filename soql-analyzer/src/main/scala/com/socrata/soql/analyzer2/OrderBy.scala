package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.typechecker.FunctionInfo
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

case class OrderBy[MT <: MetaTypes](expr: Expr[MT], ascending: Boolean, nullLast: Boolean) extends LabelUniverse[MT] {
  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]): OrderBy[MT2] =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: OrderBy[MT]): Boolean =
    this.ascending == that.ascending &&
      this.nullLast == that.nullLast &&
      this.expr.findIsomorphism(state, that.expr)

  private[analyzer2] def debugDoc(implicit ev: ExprDocProvider[MT]) =
    Seq(
      expr.debugDoc(ev),
      Seq(
        if(ascending) d"ASC" else d"DESC",
        if(nullLast) d"NULLS LAST" else d"NULLS FIRST"
      ).hsep
    ).sep.nest(2)
}

object OrderBy {
  implicit def serializeWrite[MT <: MetaTypes](implicit ev: Writable[Expr[MT]]) = new Writable[OrderBy[MT]] {
    def writeTo(buffer: WriteBuffer, ob: OrderBy[MT]): Unit = {
      buffer.write(ob.expr)
      buffer.write(ob.ascending)
      buffer.write(ob.nullLast)
    }
  }

  implicit def deserializeRead[MT <: MetaTypes](implicit ev: Readable[Expr[MT]]) = new Readable[OrderBy[MT]] {
    def readFrom(buffer: ReadBuffer): OrderBy[MT] = {
      OrderBy(
        buffer.read[Expr[MT]](),
        ascending = buffer.read[Boolean](),
        nullLast = buffer.read[Boolean]()
      )
    }
  }
}
