package com.socrata.soql.ast

import com.socrata.soql.environment.TableName
import com.socrata.soql.tokens.{LEFT, RIGHT, Token}

sealed trait Join {
  val tableName: TableName
  val expr: Expression
  val name: String

  override def toString: String = {
    s"$name ${tableName} ON $expr"
  }
}

case class LeftOuterJoin(tableName: TableName, expr: Expression) extends Join {
  val name: String = Join.LeftOuterJoinName
}

case class RightOuterJoin(tableName: TableName, expr: Expression) extends Join {
  val name: String = Join.RightOuterJoinName
}

case class InnerJoin(tableName: TableName, expr: Expression) extends Join {
  val name: String = Join.InnerJoinName
}

object Join {
  val InnerJoinName = "JOIN"
  val LeftOuterJoinName = "LEFT OUTER JOIN"
  val RightOuterJoinName = "RIGHT OUTER JOIN"
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
