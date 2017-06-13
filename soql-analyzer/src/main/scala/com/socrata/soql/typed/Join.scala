package com.socrata.soql.typed

import com.socrata.soql.environment.TableName
import com.socrata.soql.ast.Join.{InnerJoinName, LeftOuterJoinName, RightOuterJoinName}


sealed trait Join[ColumnId, Type] {
  val tableName: TableName
  val expr: CoreExpr[ColumnId, Type]
  val name: String

  override def toString: String = {
    s"$name ${tableName} ON $expr"
  }
}

case class LeftOuterJoin[ColumnId, Type](tableName: TableName, expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val name: String = LeftOuterJoinName
}

case class RightOuterJoin[ColumnId, Type](tableName: TableName, expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val name: String = RightOuterJoinName
}

case class InnerJoin[ColumnId, Type](tableName: TableName, expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val name: String = InnerJoinName
}

object Join {

  def apply[ColumnId, Type](joinType: String, tableName: TableName, expr: CoreExpr[ColumnId, Type]): Join[ColumnId, Type] = {
    joinType match {
      case InnerJoinName => InnerJoin(tableName, expr)
      case LeftOuterJoinName => LeftOuterJoin(tableName, expr)
      case RightOuterJoinName => RightOuterJoin(tableName, expr)
      case jt: String => throw new IllegalArgumentException(s"invalid join type $jt")
    }
  }
}
