package com.socrata.soql.ast


import com.socrata.soql.environment.TableName
import com.socrata.soql.tokens.{FULL, LEFT, RIGHT, Token}

sealed trait JoinType

case object InnerJoinType extends JoinType {
  override def toString(): String = JoinType.InnerJoinName
}

case object LeftOuterJoinType extends JoinType {
  override def toString(): String = JoinType.LeftOuterJoinName
}

case object RightOuterJoinType extends JoinType {
  override def toString(): String = JoinType.RightOuterJoinName
}

case object FullOuterJoinType extends JoinType {
  override def toString(): String = JoinType.FullOuterJoinName
}

object JoinType {
  val InnerJoinName = "JOIN"
  val LeftOuterJoinName = "LEFT OUTER JOIN"
  val RightOuterJoinName = "RIGHT OUTER JOIN"
  val FullOuterJoinName = "FULL OUTER JOIN"

  def apply(joinType: String): JoinType = {
    joinType match {
      case InnerJoinName => InnerJoinType
      case LeftOuterJoinName => LeftOuterJoinType
      case RightOuterJoinName => RightOuterJoinType
      case FullOuterJoinName => FullOuterJoinType
      case x => throw new IllegalArgumentException(s"invalid join type $x")
    }
  }
}

sealed trait Join {
  val tableLike: Seq[Select]
  val alias: Option[String]
  val expr: Expression
  val typ: JoinType

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(typ)
    sb.append(" ")

    tableLike match {
      case Seq(x) if x.from.nonEmpty &&
        x.selection.allUserExcept.isEmpty &&
        x.selection.allSystemExcept.isEmpty &&
        x.selection.expressions.isEmpty =>
        x.from.foreach(x => sb.append(x.toString()))
      case _ =>
        sb.append("(")
        sb.append(tableLike.map(_.toString).mkString(" |> "))
        sb.append(")")
    }

    alias.foreach { x =>
      sb.append(" AS ")
      sb.append(x.substring(TableName.SodaFountainTableNamePrefixSubStringIndex))
    }

    sb.append(" ON ")
    sb.append(expr)
    sb.toString
  }
}

case class InnerJoin(tableLike: Seq[Select], alias: Option[String], expr: Expression) extends Join {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin(tableLike: Seq[Select], alias: Option[String], expr: Expression) extends Join {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin(tableLike: Seq[Select], alias: Option[String], expr: Expression) extends Join {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin(tableLike: Seq[Select], alias: Option[String], expr: Expression) extends Join {
  val typ: JoinType = FullOuterJoinType
}

object OuterJoin {
  def apply(direction: Token, tableLike: Seq[Select], alias: Option[String], expr: Expression): Join = {
    direction match {
      case _: LEFT => LeftOuterJoin(tableLike, alias, expr)
      case _: RIGHT => RightOuterJoin(tableLike, alias, expr)
      case _: FULL => FullOuterJoin(tableLike, alias, expr)
      case t: Token => throw new IllegalArgumentException(s"invalid outer join token ${t.printable}")
    }
  }
}
