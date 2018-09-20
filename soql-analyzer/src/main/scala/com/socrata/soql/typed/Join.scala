package com.socrata.soql.typed

import com.socrata.soql._
import com.socrata.soql.ast._

sealed trait Join[ColumnId, Type] {
  val from: JoinAnalysis[ColumnId, Type]
  val on: CoreExpr[ColumnId, Type]
  val typ: JoinType

  def useTableQualifier[ColumnId](id: ColumnId, qual: Qualifier) = (id, qual.orElse(Some(from.fromTable.name)))

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): Join[NewColumnId, Type] = {
    def fWithTable(id: ColumnId, qual: Qualifier) = f(id, qual.orElse(Some(from.fromTable.name)))

    val mappedSub = from.subAnalysis.map {
      case SubAnalysis(h :: tail, alias) =>
        val firstAna = h.mapColumnIds(fWithTable)
        val restAnas = tail.map(_.mapColumnIds(f))
        SubAnalysis(firstAna :: restAnas, alias)
    }

    typed.Join(typ, JoinAnalysis(from.fromTable, mappedSub), on.mapColumnIds(f))
  }

  override def toString: String = {
    s"$typ $from ON $on"
  }
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
