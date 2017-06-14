package com.socrata.soql.ast

import com.socrata.soql.environment.TableName
import com.socrata.soql.tokens.{LEFT, RIGHT, Token}

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

object JoinType {
  val InnerJoinName = "JOIN"
  val LeftOuterJoinName = "LEFT OUTER JOIN"
  val RightOuterJoinName = "RIGHT OUTER JOIN"

  def apply(joinType: String): JoinType = {
    joinType match {
      case InnerJoinName => InnerJoinType
      case LeftOuterJoinName => LeftOuterJoinType
      case RightOuterJoinName => RightOuterJoinType
      case x => throw new IllegalArgumentException(s"invalid join type $x")
    }
  }
}

sealed trait Join {
  val tableName: TableName
  val expr: Expression
  val typ: JoinType

  override def toString: String = {
    s"${typ.toString} ${tableName} ON $expr"
  }
}

case class InnerJoin(tableName: TableName, expr: Expression) extends Join {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin(tableName: TableName, expr: Expression) extends Join {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin(tableName: TableName, expr: Expression) extends Join {
  val typ: JoinType = RightOuterJoinType
}

object OuterJoin {
  def apply(direction: Token, tableName: TableName, expr: Expression): Join = {
    direction match {
      case _: LEFT => LeftOuterJoin(tableName, expr)
      case _: RIGHT => RightOuterJoin(tableName, expr)
      case t: Token => throw new IllegalArgumentException(s"invalid outer join token ${t.printable}")
    }
  }
}
