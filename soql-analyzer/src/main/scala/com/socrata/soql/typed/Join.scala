package com.socrata.soql.typed

import com.socrata.soql.environment.TableName
import com.socrata.soql.ast.{InnerJoinType, JoinType, LeftOuterJoinType, RightOuterJoinType}


sealed trait Join[ColumnId, Type] {
  val tableName: TableName
  val expr: CoreExpr[ColumnId, Type]
  val typ: JoinType

  override def toString: String = {
    s"${typ.toString} ${tableName} ON $expr"
  }
}

case class InnerJoin[ColumnId, Type](tableName: TableName, expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin[ColumnId, Type](tableName: TableName, expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin[ColumnId, Type](tableName: TableName, expr: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = RightOuterJoinType
}

object Join {

  def apply[ColumnId, Type](joinType: JoinType, tableName: TableName, expr: CoreExpr[ColumnId, Type]): Join[ColumnId, Type] = {
    joinType match {
      case InnerJoinType => InnerJoin(tableName, expr)
      case LeftOuterJoinType => LeftOuterJoin(tableName, expr)
      case RightOuterJoinType => RightOuterJoin(tableName, expr)
    }
  }
}
