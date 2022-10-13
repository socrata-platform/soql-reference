package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.typechecker.{FunctionInfo, HasType, HasDoc}
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

case class OrderBy[+CT, +CV](expr: Expr[CT, CV], ascending: Boolean, nullLast: Boolean) {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(expr = expr.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(expr = expr.doRelabel(state))

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: OrderBy[CT2, CV2]): Boolean =
    this.ascending == that.ascending &&
      this.nullLast == that.nullLast &&
      this.expr.findIsomorphism(state, that.expr)

  def debugDoc(implicit ev: HasDoc[CV]) =
    Seq(
      expr.debugDoc,
      Seq(
        if(ascending) d"ASC" else d"DESC",
        if(nullLast) d"NULLS LAST" else d"NULLS FIRST"
      ).hsep
    ).sep.nest(2)
}

object OrderBy {
  implicit def serializeWrite[CT : Writable, CV: Writable] = new Writable[OrderBy[CT, CV]] {
    def writeTo(buffer: WriteBuffer, ob: OrderBy[CT, CV]): Unit = {
      buffer.write(ob.expr)
      buffer.write(ob.ascending)
      buffer.write(ob.nullLast)
    }
  }

  implicit def deserializeRead[CT, CV](implicit ev: Readable[Expr[CT, CV]]) = new Readable[OrderBy[CT, CV]] {
    def readFrom(buffer: ReadBuffer): OrderBy[CT, CV] = {
      OrderBy(
        buffer.read[Expr[CT, CV]](),
        ascending = buffer.read[Boolean](),
        nullLast = buffer.read[Boolean]()
      )
    }
  }
}
