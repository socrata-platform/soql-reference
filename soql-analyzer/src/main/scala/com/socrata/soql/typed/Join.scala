package com.socrata.soql.typed

import com.socrata.NonEmptySeq
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
      case SubAnalysis(NonEmptySeq(head, tail), alias) =>
        val newAnas = NonEmptySeq(head.mapColumnIds(fWithTable), tail.map(_.mapColumnIds(f)))
        SubAnalysis(newAnas, alias)
    }

    typed.Join(typ, JoinAnalysis(from.fromTable, mappedSub), on.mapColumnIds(f))
  }

  override def toString: String = {
    s"$typ $from ON $on"
  }

  // joins are simple if there is no subAnalysis ("join @aaaa-aaaa[ as a]") or if all analyses in subAnalysis
  // are select * "join (select * from @aaaa-aaaa) as a"
  def isSimple: Boolean = {
    from.subAnalysis.forall(_.analyses.seq.forall(_.selection.keys.isEmpty))
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

  def expandJoins[ColumnId, Type](analyses: Seq[SoQLAnalysis[ColumnId, Type]]): Seq[typed.Join[ColumnId, Type]] = {
    def expandJoin(join: typed.Join[ColumnId, Type]): Seq[typed.Join[ColumnId, Type]] = {
      if (join.isSimple) Seq(join)
      else expandJoins(join.from.analyses) :+ join
    }

    analyses.flatMap(_.joins.flatMap(expandJoin))
  }
}
