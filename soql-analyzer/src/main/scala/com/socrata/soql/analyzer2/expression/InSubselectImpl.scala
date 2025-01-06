package com.socrata.soql.analyzer2.expression

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

trait InSubselectImpl[MT <: MetaTypes] { this: InSubselect[MT] =>
  type Self[MT2 <: MetaTypes] = InSubselect[MT2]

  private[analyzer2] def columnReferences: Map[AutoTableLabel,Set[ColumnLabel]] = ??? // implements `private[package analyzer2] def columnReferences: Map[InSubselect.this.AutoTableLabel,Set[InSubselect.this.ColumnLabel]]`
  protected def doDebugDoc(implicit ev: StatementDocProvider[MT]): Doc[Annotation[MT]] =
    Seq(substatement.doDebugDoc(ev)).encloseHanging(scrutinee.debugDoc(ev) +#+ d"IN (", d",", d")")
  private[analyzer2] def doRelabel(state: RelabelState): InSubselect[MT] = ??? // implements `private[package analyzer2] def doRelabel(state: com.socrata.soql.analyzer2.RelabelState): InSubselect.this.Self[MT]`
  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](expr: RewriteDatabaseNamesState[MT2]): InSubselect[MT2] = ??? // implements `private[package analyzer2] def doRewriteDatabaseNames[MT2 <: com.socrata.soql.analyzer2.MetaTypes](expr: InSubselect.this.RewriteDatabaseNamesState[MT2]): InSubselect.this.Self[MT2]`
  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = ???
  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = ??? // implements `private[package analyzer2] def findIsomorphism(state: InSubselect.this.IsomorphismState, that: com.socrata.soql.analyzer2.Expr[MT]): Boolean`
  def isAggregated: Boolean = false
  def isWindowed: Boolean = false
  private[analyzer2] def reReference(reference: Source): InSubselect[MT] = ??? // implements `private[package analyzer2] def reReference(reference: InSubselect.this.Source): InSubselect.this.Self[MT]`
  val size: Int = 1 + scrutinee.size + 0 /* ... what does the size of a subquery even mean? */
}

trait OInSubselectImpl { this: InSubselect.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableExpr: Writable[Expr[MT]]): Writable[InSubselect[MT]] = new Writable[InSubselect[MT]] with ExpressionUniverse[MT] {
    def writeTo(buffer: WriteBuffer, fc: InSubselect): Unit = {
      buffer.write(fc.scrutinee)
      buffer.write(fc.not)
      // buffer.write(fc.subselect)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableExpr: Readable[Expr[MT]]): Readable[InSubselect[MT]] = new Readable[InSubselect[MT]] with ExpressionUniverse[MT] {
    def readFrom(buffer: ReadBuffer): InSubselect = {
      val scrutinee = buffer.read[Expr]()
      val not = buffer.read[Boolean]()
      // val subselect = buffer.read[Statement]()
      ???
    }
  }
}
