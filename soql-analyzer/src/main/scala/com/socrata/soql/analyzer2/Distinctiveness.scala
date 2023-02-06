package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed trait Distinctiveness[MT <: MetaTypes] extends LabelUniverse[MT] {
  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]): Distinctiveness[MT2]
  private[analyzer2] def doRelabel(state: RelabelState): Distinctiveness[MT]
  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Distinctiveness[MT]): Boolean
  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]]
  private[analyzer2] def debugDoc(implicit ev: ExprDocProvider[MT]): Option[Doc[Annotation[MT]]]
}
object Distinctiveness {
  case class Indistinct[MT <: MetaTypes]() extends Distinctiveness[MT] {
    private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
      this.asInstanceOf[Indistinct[MT2]] // SAFETY: this contains no column labesls
    private[analyzer2] def doRelabel(state: RelabelState) = this
    private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Distinctiveness[MT]): Boolean =
      that == this
    private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = Map.empty

    private[analyzer2] def debugDoc(implicit ev: ExprDocProvider[MT]) = None
  }

  case class FullyDistinct[MT <: MetaTypes]() extends Distinctiveness[MT] {
    private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
      this.asInstanceOf[FullyDistinct[MT2]] // SAFETY: this contains no column labesls
    private[analyzer2] def doRelabel(state: RelabelState) = this
    private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Distinctiveness[MT]): Boolean =
      that == this

    // Uggh.. this is a little weird (and the reason this method is
    // "private[analyzer2]") since FullyDistinct kind of references
    // all columns in the select in which it appears.
    private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = Map.empty

    private[analyzer2] def debugDoc(implicit ev: ExprDocProvider[MT]) = Some(d"DISTINCT")
  }

  case class On[MT <: MetaTypes](exprs: Seq[Expr[MT]]) extends Distinctiveness[MT] {
    private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
      On(exprs.map(_.doRewriteDatabaseNames(state)))
    private[analyzer2] def doRelabel(state: RelabelState) =
      On(exprs.map(_.doRelabel(state)))

    private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
      exprs.foldLeft(Map.empty[AutoTableLabel, Set[ColumnLabel]]) { (acc, e) =>
        acc.mergeWith(e.columnReferences)(_ ++ _)
      }

    private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Distinctiveness[MT]): Boolean =
      that match {
        case On(thatExprs) =>
          this.exprs.length == thatExprs.length &&
            this.exprs.zip(thatExprs).forall { case (a, b) => a.findIsomorphism(state, b) }
        case _ => false
      }

    private[analyzer2] def debugDoc(implicit ev: ExprDocProvider[MT]) =
      Some(exprs.map(_.debugDoc(ev)).encloseNesting(d"DISTINCT ON (", d",", d")"))
  }

  implicit def serialize[MT <: MetaTypes](implicit ev: Writable[Expr[MT]]) = new Writable[Distinctiveness[MT]] {
    def writeTo(buffer: WriteBuffer, d: Distinctiveness[MT]): Unit = {
      d match {
        case Indistinct() =>
          buffer.write(0)
        case FullyDistinct() =>
          buffer.write(1)
        case On(exprs) =>
          buffer.write(2)
          buffer.write(exprs)
      }
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit ev: Readable[Expr[MT]]) = new Readable[Distinctiveness[MT]] {
    def readFrom(buffer: ReadBuffer): Distinctiveness[MT] = {
      buffer.read[Int]() match {
        case 0 => Indistinct()
        case 1 => FullyDistinct()
        case 2 => On(buffer.read[Seq[Expr[MT]]]())
        case other => fail("Unknown distinctiveness tag " + other)
      }
    }
  }
}
