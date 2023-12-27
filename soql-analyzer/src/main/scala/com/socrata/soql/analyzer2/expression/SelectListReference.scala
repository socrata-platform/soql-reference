package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

trait SelectListReferenceImpl[MT <: MetaTypes] { this: SelectListReference[MT] =>
  type Self[MT <: MetaTypes] = SelectListReference[MT]

  val size = 1

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    throw new Exception("Cannot ask for ColumnReferences on a query with SelectListReferences")

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    this.asInstanceOf[SelectListReference[MT2]] // SAFETY: this contains no column labels

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = {
    this == that
  }

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) =
    Doc(index).annotate(Annotation.SelectListReference[MT](index))

  private[analyzer2] def reReference(reference: Source): Self[MT] =
    copy()(position = position.reReference(reference))

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}

trait OSelectListReferenceImpl { this: SelectListReference.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableCT : Writable[MT#ColumnType], writableRNS: Writable[MT#ResourceNameScope]) = new Writable[SelectListReference[MT]] {
    def writeTo(buffer: WriteBuffer, slr: SelectListReference[MT]): Unit = {
      buffer.write(slr.index)
      buffer.write(slr.isAggregated)
      buffer.write(slr.isWindowed)
      buffer.write(slr.typ)
      buffer.write(slr.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT : Readable[MT#ColumnType], readableRNS: Readable[MT#ResourceNameScope]) = new Readable[SelectListReference[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): SelectListReference[MT] = {
      SelectListReference(
        index = buffer.read[Int](),
        isAggregated = buffer.read[Boolean](),
        isWindowed = buffer.read[Boolean](),
        typ = buffer.read[MT#ColumnType]()
      )(
        buffer.read[AtomicPositionInfo]()
      )
    }
  }
}
