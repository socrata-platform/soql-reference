package com.socrata.soql.ast


import com.socrata.soql.environment.{HoleName, TableName}
import com.socrata.soql.tokens.{FULL, LEFT, RIGHT, Token}
import com.socrata.prettyprint.prelude._

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
  def isSimple = from.isInstanceOf[JoinTable]

  override def toString: String = {
    doc.layoutSmart(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString
  }

  def doc: Doc[Nothing] =
    (Doc(typ.toString) ++ (if(lateral) d" LATERAL" else d"") +#+ from.doc +#+ d"ON" +#+ on.doc).hang(2)

  def replaceHoles(f: Hole => Expression): Join
  def collectHoles(f: PartialFunction[Hole, Expression]): Join
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): Join
}

object Join {
  def expandJoins(selects: Seq[Select]): Seq[Join] = {
    def expandJoin(join: Join): Seq[Join] = {
      join.from match {
        case JoinTable(_) => Seq(join)
        case JoinFunc(_, _) => Seq(join)
        case JoinQuery(selects, _) => expandJoins(selects.seq) :+ join
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
  def replaceHoles(f: Hole => Expression): InnerJoin =
    InnerJoin(from.replaceHoles(f), on.replaceHoles(f), lateral)
  def collectHoles(f: PartialFunction[Hole, Expression]): InnerJoin =
    InnerJoin(from.collectHoles(f), on.collectHoles(f), lateral)
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): Join =
    copy(from = from.rewriteJoinFuncs(f, aliasProvider))
}

case class LeftOuterJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = LeftOuterJoinType
  def replaceHoles(f: Hole => Expression): LeftOuterJoin =
    LeftOuterJoin(from.replaceHoles(f), on.replaceHoles(f), lateral)
  def collectHoles(f: PartialFunction[Hole, Expression]): LeftOuterJoin =
    LeftOuterJoin(from.collectHoles(f), on.collectHoles(f), lateral)
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): Join =
    copy(from = from.rewriteJoinFuncs(f, aliasProvider))
}

case class RightOuterJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = RightOuterJoinType
  def replaceHoles(f: Hole => Expression): RightOuterJoin =
    RightOuterJoin(from.replaceHoles(f), on.replaceHoles(f), lateral)
  def collectHoles(f: PartialFunction[Hole, Expression]): RightOuterJoin =
    RightOuterJoin(from.collectHoles(f), on.collectHoles(f), lateral)
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): Join =
    copy(from = from.rewriteJoinFuncs(f, aliasProvider))
}

case class FullOuterJoin(from: JoinSelect, on: Expression, lateral: Boolean) extends Join {
  val typ: JoinType = FullOuterJoinType
  def replaceHoles(f: Hole => Expression): FullOuterJoin =
    FullOuterJoin(from.replaceHoles(f), on.replaceHoles(f), lateral)
  def collectHoles(f: PartialFunction[Hole, Expression]): FullOuterJoin =
    FullOuterJoin(from.collectHoles(f), on.collectHoles(f), lateral)
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): Join =
    copy(from = from.rewriteJoinFuncs(f, aliasProvider))
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
