package com.socrata.soql.typed

import com.socrata.soql._
import com.socrata.soql.ast._
import com.socrata.soql.environment.TableName

sealed trait Join[ColumnId, Type] {
  val from: JoinAnalysis[ColumnId, Type]
  val on: CoreExpr[ColumnId, Type]
  val typ: JoinType
  val lateral: Boolean

  def useTableQualifier[ColumnId](id: ColumnId, qual: Qualifier) = (id, qual.orElse(from.subAnalysis.left.toOption.map(_.name)))

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): Join[NewColumnId, Type] = {
    val mappedSub: Either[TableName, SubAnalysis[NewColumnId, Type]] = from.subAnalysis match {
      case Right(SubAnalysis(analyses, alias)) =>
        val newAnas = analyses.flatMap { analysis => Leaf(analysis.mapColumnIds(f)) }
        Right(SubAnalysis(newAnas, alias))
      case Left(tableName) =>
        Left(tableName)

    }

    typed.Join(typ, JoinAnalysis(mappedSub), on.mapColumnIds(f), lateral)
  }

  def copy(from: JoinAnalysis[ColumnId, Type] = from, on: CoreExpr[ColumnId, Type] = on, lateral: Boolean): typed.Join[ColumnId, Type] = {
    typed.Join(this.typ, from, on, lateral)
  }

  override def toString: String = {
    s"$typ $from ON $on"
  }

  // joins are simple if there is no subAnalysis, e.g. "join @aaaa-aaaa[ as a]"
  def isSimple: Boolean = from.subAnalysis.isLeft

}

case class InnerJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type], lateral: Boolean) extends Join[ColumnId, Type] {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type], lateral: Boolean) extends Join[ColumnId, Type] {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type], lateral: Boolean) extends Join[ColumnId, Type] {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin[ColumnId, Type](from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type], lateral: Boolean) extends Join[ColumnId, Type] {
  val typ: JoinType = FullOuterJoinType
}

object Join {

  def apply[ColumnId, Type](joinType: JoinType, from: JoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type], lateral: Boolean): Join[ColumnId, Type] = {
    joinType match {
      case InnerJoinType => typed.InnerJoin(from, on, lateral)
      case LeftOuterJoinType => typed.LeftOuterJoin(from, on, lateral)
      case RightOuterJoinType => typed.RightOuterJoin(from, on, lateral)
      case FullOuterJoinType => typed.FullOuterJoin(from, on, lateral)
    }
  }

  def expandJoins[ColumnId, Type](analyses: Seq[SoQLAnalysis[ColumnId, Type]]): Seq[typed.Join[ColumnId, Type]] = {
    def expandJoin(join: typed.Join[ColumnId, Type]): Seq[typed.Join[ColumnId, Type]] = {
      join.from.subAnalysis match {
        case Left(_) => Seq(join)
        case Right(SubAnalysis(analyses, _)) => expandJoins(analyses.seq) :+ join
      }
    }

    analyses.flatMap(_.joins.flatMap(expandJoin))
  }
}
