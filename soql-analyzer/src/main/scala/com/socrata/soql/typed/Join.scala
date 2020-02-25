package com.socrata.soql.typed

import com.socrata.soql.collection.NonEmptySeq
import com.socrata.soql._
import com.socrata.soql.ast._

case class Join[ColumnId, QualifiedColumnId, Type](typ: JoinType, from: JoinAnalysis[ColumnId, QualifiedColumnId, Type], on: CoreExpr[QualifiedColumnId, Type]) {
  def mapColumnIds[NewColumnId, NewQualifiedColumnId](f: ColumnIdTransform[ColumnId, QualifiedColumnId, NewColumnId, NewQualifiedColumnId]): Join[NewColumnId, NewQualifiedColumnId, Type] = {
    typed.Join(typ, from.mapColumnIds(f), on.mapColumnIds(f.mapQualifiedColumnId))
  }

  def mapAccumColumnIds[State, NewColumnId, NewQualifiedColumnId](s0: State)(f: ColumnIdTransformAccum[State, ColumnId, QualifiedColumnId, NewColumnId, NewQualifiedColumnId]): (State, Join[NewColumnId, NewQualifiedColumnId, Type]) = {
    val (s1, newFrom) = from.mapAccumColumnIds(s0)(f)
    val (s2, newOn) = on.mapAccumColumnIds(s1)(f.mapQualifiedColumnId)
    (s2, typed.Join(typ, newFrom, newOn))
  }

  override def toString: String =
    s"$typ $from ON $on"
}

object Join {
  def inner[ColumnId, QualifiedColumnId, Type](from: JoinAnalysis[ColumnId, QualifiedColumnId, Type], on: CoreExpr[QualifiedColumnId, Type]) =
    Join(InnerJoinType, from, on)
  def leftOuter[ColumnId, QualifiedColumnId, Type](from: JoinAnalysis[ColumnId, QualifiedColumnId, Type], on: CoreExpr[QualifiedColumnId, Type]) =
    Join(LeftOuterJoinType, from, on)
  def rightOuter[ColumnId, QualifiedColumnId, Type](from: JoinAnalysis[ColumnId, QualifiedColumnId, Type], on: CoreExpr[QualifiedColumnId, Type]) =
    Join(RightOuterJoinType, from, on)
  def fullOuter[ColumnId, QualifiedColumnId, Type](from: JoinAnalysis[ColumnId, QualifiedColumnId, Type], on: CoreExpr[QualifiedColumnId, Type]) =
    Join(RightOuterJoinType, from, on)
}
