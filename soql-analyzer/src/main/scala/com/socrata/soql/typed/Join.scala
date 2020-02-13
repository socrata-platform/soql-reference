package com.socrata.soql.typed

import com.socrata.soql.collection.NonEmptySeq
import com.socrata.soql._
import com.socrata.soql.ast._

sealed trait Join[ColumnId, Type] {
  val from: JoinAnalysis[ColumnId, Type]
  val on: CoreExpr[ColumnId, Type]
  val typ: JoinType

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId): Join[NewColumnId, Type] = {
    typed.Join(typ, from.mapColumnIds(f), on.mapColumnIds(f))
  }

  def mapAccumColumnIds[State,NewColumnId](s0: State)(f: (State, ColumnId) => (State, NewColumnId)): (State, Join[NewColumnId, Type]) = {
    val (s1, newFrom) = from.mapAccumColumnIds(s0)(f)
    val (s2, newOn) = on.mapAccumColumnIds(s1)(f)
    (s2, typed.Join(typ, newFrom, newOn))
  }

  override def toString: String =
    s"$typ $from ON $on"
}

case class InnerJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = FullOuterJoinType
}

object Join {
  def apply[ColumnId, Type](joinType: JoinType, from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type]): Join[ColumnId, Type] = {
    joinType match {
      case InnerJoinType => typed.InnerJoin(from, on)
      case LeftOuterJoinType => typed.LeftOuterJoin(from, on)
      case RightOuterJoinType => typed.RightOuterJoin(from, on)
      case FullOuterJoinType => typed.FullOuterJoin(from, on)
    }
  }
}
