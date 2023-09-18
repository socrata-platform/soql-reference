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

  private[analyzer2] def findIsomorphismish(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT],
    recurseStmt: (Statement[MT], IsomorphismState, Option[AutoTableLabel], Option[AutoTableLabel], Statement[MT]) => Boolean,
    recurseFrom: (From[MT], IsomorphismState, From[MT]) => Boolean,
  ): Boolean =
    that match {
      case CombinedTables(_, thatLeft, thatRight) =>
        this.left.findIsomorphismish(state, thisCurrentTableLabel, thatCurrentTableLabel, thatLeft, recurseStmt, recurseFrom) &&
          this.right.findIsomorphismish(state, thisCurrentTableLabel, thatCurrentTableLabel, thatRight, recurseStmt, recurseFrom)
      case _ =>
        false
    }

  private[analyzer2] def findVerticalSlice(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT]
  ): Boolean =
    that match {
      case CombinedTables(TableFunc.UnionAll, thatLeft, thatRight) =>
        // This is the only case where we can actually do a subset
        // operation (all other ops care about all values in the
        // selected row).  Now, this is tricky because we don't care
        // about column ordering, so say we have
        //   (select x) union (select y)
        // and the "that" that we're looking at is
        //   (select a, x) union all (select y, b)
        // in that case, we want to say "no, this is not a subset" but
        // a simple "find subset left, find subset right" will say
        // "yep, the left is a subset and the right is a subset,
        // therefore this whole thing is a subset!" which is wrong.
        //
        // As an even trickier case, say the "that" was
        //   (select x, x) union all (select x, y)
        // In that case we want to say "yes this is a subset and the
        // "x" column in this corresponds to the _second_ column in
        // "that".  This DOES NOT HANDLE that case and just says "No,
        // I don't think this is a subset".  Hopefully this is
        // edge-casey enough to be irrelevant.
        if(!this.left.findVerticalSlice(state, thisCurrentTableLabel, thatCurrentTableLabel, thatLeft)) {
          return false
        }

        // Ok, we have a left-subset.  Good.  Now we need to see if
        // the _corresponding columns_ on the RHS also form such a
        // subset.
        if(!this.right.findVerticalSlice(state, thisCurrentTableLabel, thatCurrentTableLabel, thatRight)) {
          return false
        }

        this.schema.keysIterator.zipWithIndex.forall { case (thisLabel, thisLabelIdx) =>
          val (_, thatLabel) = state.mapFrom(thisCurrentTableLabel, thisLabel).getOrElse {
            throw new Exception("We've found a vertical slice but it didn't end up in the state??")
          }
          val thatLabelIdx = that.schema.keysIterator.indexOf(thatLabel)
          if(thatLabelIdx == -1) throw new Exception("We've found a vertical slice, but couldn't find the corresponding column??")
          val thisRightLabel = this.right.schema.keysIterator.drop(thisLabelIdx).next()
          state.mapFrom(thisCurrentTableLabel, thisRightLabel) match {
            case Some((_, thatRightLabel)) =>
              thatRight.schema.keysIterator.indexOf(thatRightLabel) == thatLabelIdx
            case None =>
              false
          }
        }
      case _ =>
        // We care about everything here, but on recursing further we
        // can go back to only caring about subsets.
        findIsomorphismish(
          state,
          thisCurrentTableLabel,
          thatCurrentTableLabel,
          that,
          (stmt, isostate, thisLbl, thatLbl, that) => stmt.findVerticalSlice(isostate, thisLbl, thatLbl, that),
          (from, isostate, that) => from.findVerticalSlice(isostate, that)
        )
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
