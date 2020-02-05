package com.socrata.soql.ast

import scala.runtime.ScalaRunTime

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
}

sealed trait Join extends Product {
  val from: JoinSource
  def name: TableName = from.name
  def outputTableIdentifier: Int = from.outputTableIdentifier
  val on: Expression
  val typ: JoinType

  def allTableReferences = from.allTableReferences

  override def toString: String = {
    if(AST.pretty) {
      s"$typ $from ON $on"
    } else {
      ScalaRunTime._toString(this)
    }
  }
}

object Join {
  def apply(joinType: JoinType, from: JoinSource, on: Expression): Join = {
    joinType match {
      case InnerJoinType => InnerJoin(from, on)
      case LeftOuterJoinType => LeftOuterJoin(from, on)
      case RightOuterJoinType => RightOuterJoin(from, on)
      case FullOuterJoinType => FullOuterJoin(from, on)
    }
  }
}

case class InnerJoin(from: JoinSource, on: Expression) extends Join {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin(from: JoinSource, on: Expression) extends Join {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin(from: JoinSource, on: Expression) extends Join {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin(from: JoinSource, on: Expression) extends Join {
  val typ: JoinType = FullOuterJoinType
}

object OuterJoin {
  val dirToJoinType: Map[Token, JoinType] = Map(
    LEFT() -> LeftOuterJoinType,
    RIGHT() -> RightOuterJoinType,
    FULL() -> FullOuterJoinType
  )

  def apply(direction: Token, from: JoinSource, on: Expression): Join = {
    dirToJoinType.get(direction).map { joinType =>
      Join(joinType, from, on)
    }.getOrElse {
      throw new IllegalArgumentException(s"invalid outer join token ${direction.printable}")
    }
  }
}
