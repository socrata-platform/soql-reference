package com.socrata.soql.analyzer2.statement

import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

import DocUtils._

trait CombinedTablesImpl[MT <: MetaTypes] { this: CombinedTables[MT] =>
  type Self[MT <: MetaTypes] = CombinedTables[MT]

  def asSelf = this

  val schema = left.schema

  def unique = LazyList.empty

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    left.find(predicate).orElse(right.find(predicate))

  def contains(e: Expr[MT]): Boolean =
    left.contains(e) || right.contains(e)

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] =
    right.doAllTables(left.doAllTables(set))

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] =
    left.realTables ++ right.realTables

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    left.columnReferences.mergeWith(right.columnReferences)(_ ++ _)

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy(
      left = left.doRewriteDatabaseNames(state),
      right = right.doRewriteDatabaseNames(state)
    )

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT] =
    copy(left = left.doRelabel(state), right = right.doRelabel(state))

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    left.doLabelMap(state)
    right.doLabelMap(state)
  }

  private[analyzer2] def findIsomorphism(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT]
  ): Boolean =
    that match {
      case CombinedTables(_, thatLeft, thatRight) =>
        this.left.findIsomorphism(state, thisCurrentTableLabel, thatCurrentTableLabel, thatLeft) &&
          this.right.findIsomorphism(state, thisCurrentTableLabel, thatCurrentTableLabel, thatRight)
      case _ =>
        false
    }

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(left = left.mapAlias(f), right = right.mapAlias(f))

  private[analyzer2] override def doDebugDoc(implicit ev: StatementDocProvider[MT]): Doc[Annotation[MT]] = {
    left.doDebugDoc.encloseNesting(d"(", d")") +#+ op.debugDoc +#+ right.doDebugDoc.encloseNesting(d"(", d")")
  }
}

trait OCombinedTablesImpl { this: CombinedTables.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[CombinedTables[MT]] =
    new Writable[CombinedTables[MT]] {
      def writeTo(buffer: WriteBuffer, ct: CombinedTables[MT]): Unit = {
        buffer.write(ct.op)
        buffer.write(ct.left)
        buffer.write(ct.right)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[CombinedTables[MT]] =
    new Readable[CombinedTables[MT]] {
      def readFrom(buffer: ReadBuffer): CombinedTables[MT] = {
        CombinedTables(
          op = buffer.read[TableFunc](),
          left = buffer.read[Statement[MT]](),
          right = buffer.read[Statement[MT]]()
        )
      }
    }
}
