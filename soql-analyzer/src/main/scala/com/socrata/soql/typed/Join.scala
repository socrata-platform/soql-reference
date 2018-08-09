package com.socrata.soql.typed

import com.socrata.soql.{SimpleSoQLAnalysis, SoQLAnalysis}
import com.socrata.soql.ast._
import com.socrata.soql.environment.TableName

object X extends App {
  println("hi")
}


sealed trait Join[ColumnId, Type] {
  val tableLike: Seq[SoQLAnalysis[ColumnId, Type]]
  val alias: Option[String]
  val expr: CoreExpr[ColumnId, Type]
  val typ: JoinType

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(typ.toString)
    sb.append(" ")
    SimpleSoQLAnalysis.asSoQL(tableLike) match {
      case Some(x) =>
        sb.append(x.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix))
      case None =>
        sb.append("(")
        sb.append(tableLike.map(_.toString).mkString(" |> "))
        sb.append(")")
    }

    alias.foreach { x =>
      sb.append(" AS ")
      sb.append(x.substring(TableName.SodaFountainTableNamePrefixSubStringIndex))
    }

    sb.append(" ON ")
    sb.append(expr.toString)
    sb.toString
  }
}

case class InnerJoin[ColumnId, Type](tableLike: Seq[SoQLAnalysis[ColumnId, Type]], alias: Option[String], expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin[ColumnId, Type](tableLike: Seq[SoQLAnalysis[ColumnId, Type]], alias: Option[String], expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin[ColumnId, Type](tableLike: Seq[SoQLAnalysis[ColumnId, Type]], alias: Option[String], expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin[ColumnId, Type](tableLike: Seq[SoQLAnalysis[ColumnId, Type]], alias: Option[String], expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = FullOuterJoinType
}

object Join {

  def apply[ColumnId, Type](joinType: JoinType, tableLike: Seq[SoQLAnalysis[ColumnId, Type]], alias: Option[String], expr: CoreExpr[ColumnId, Type]): Join[ColumnId, Type] = {
    joinType match {
      case InnerJoinType => InnerJoin(tableLike, alias, expr)
      case LeftOuterJoinType => LeftOuterJoin(tableLike, alias, expr)
      case RightOuterJoinType => RightOuterJoin(tableLike, alias, expr)
      case FullOuterJoinType => FullOuterJoin(tableLike, alias, expr)
    }
  }
}
