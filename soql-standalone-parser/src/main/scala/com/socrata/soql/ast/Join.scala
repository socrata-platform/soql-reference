package com.socrata.soql.ast


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
  val from: JoinSelect
  val on: Expression
  val typ: JoinType

  def isSimple = SimpleSelect.isSimple(from.selects)

  override def toString: String = {
    s"$typ $from ON $on"
  }


}

case class InnerJoin(from: JoinSelect, on: Expression) extends Join {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin(from: JoinSelect, on: Expression) extends Join {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin(from: JoinSelect, on: Expression) extends Join {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin(from: JoinSelect, on: Expression) extends Join {
  val typ: JoinType = FullOuterJoinType
}

object OuterJoin {
  def apply(direction: Token, from: JoinSelect, on: Expression): Join = {
    direction match {
      case _: LEFT => LeftOuterJoin(from, on)
      case _: RIGHT => RightOuterJoin(from, on)
      case _: FULL => FullOuterJoin(from, on)
      case t: Token => throw new IllegalArgumentException(s"invalid outer join token ${t.printable}")
    }
  }
}
