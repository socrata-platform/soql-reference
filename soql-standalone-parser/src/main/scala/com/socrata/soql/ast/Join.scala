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
      case x => throw new IllegalArgumentException(s"invalid join type: $x")
    }
  }
}

sealed trait Join {
  val from: JoinSelect
  val on: Expression
  val typ: JoinType
  val lateral: Boolean

  // joins are simple if there is no subAnalysis, e.g. "join @aaaa-aaaa[ as a]"
  def isSimple = from.subSelect.isLeft

  override def toString: String = {
    s"$typ $from ON $on"
  }
}

object Join {
  def expandJoins(selects: Seq[Select]): Seq[Join] = {
    def expandJoin(join: Join): Seq[Join] = {
      join.from.subSelect match {
        case Left(_) => Seq(join)
        case Right(s) => expandJoins(s.selects.seq) :+ join
      }
    }

    selects.flatMap(_.joins.flatMap(expandJoin))
  }

  def apply(joinType: JoinType, from: JoinSelect, on: Expression, lateral: Boolean): Join = {
    joinType match {
      case InnerJoinType => InnerJoin(from, on, lateral)
      case LeftOuterJoinType => LeftOuterJoin(from, on, lateral)
      case RightOuterJoinType => RightOuterJoin(from, on, lateral)
      case FullOuterJoinType => FullOuterJoin(from, on, lateral)
    }
  }
}

case class InnerJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = FullOuterJoinType
}

object OuterJoin {
  val dirToJoinType: Map[Token, JoinType] = Map(
    LEFT() -> LeftOuterJoinType,
    RIGHT() -> RightOuterJoinType,
    FULL() -> FullOuterJoinType
  )

  def apply(direction: Token, from: JoinSelect, on: Expression, lateral: Boolean): Join = {
    dirToJoinType.get(direction).map { joinType =>
      Join(joinType, from, on, lateral)
    }.getOrElse {
      throw new IllegalArgumentException(s"invalid outer join token ${direction.printable}")
    }
  }
}
