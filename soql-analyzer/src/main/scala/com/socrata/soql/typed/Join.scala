package com.socrata.soql.typed

import com.socrata.soql.{SimpleSoQLAnalysis, SoQLAnalysis}
import com.socrata.soql.ast._
import com.socrata.soql.environment.TableName

object X extends App {
  println("hi")
}


trait TableSource[ColumnId, Type]

// TODO: can these just extend TableSource[Nothing, Nothing]?
case class TableName[ColumnId, Type](name: String) extends TableSource[ColumnId, Type] {
  override def toString: String = {
    val unPrefixedName = name.substring(TableName.SodaFountainTableNamePrefixSubStringIndex)
    s"@$unPrefixedName"
  }
}
class NoContext[ColumnId, Type] extends TableSource[ColumnId, Type]

// TODO: does this From need an alias? what's the point?
case class From[ColumnId, Type](source: TableSource[ColumnId, Type], refs: List[SoQLAnalysis[ColumnId, Type]], alias: Option[String])

sealed trait Join[ColumnId, Type] {
  val from: From[ColumnId, Type]
  val on: CoreExpr[ColumnId, Type]
  val typ: JoinType

//  override def toString: String = {
//    val sb = new StringBuilder
//    sb.append(typ.toString)
//    sb.append(" ")
//    from match {
//      case From(TableName(name), _, _) => name.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix)
//      case f =>
//        sb.append("(")
//        sb.append(f.map(_.toString).mkString(" |> "))
//        sb.append(")")
//    }
//    SimpleSoQLAnalysis.asSoQL(from) match {
//      case Some(x) =>
//        sb.append(x.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix))
//      case None =>
//        sb.append("(")
//        sb.append(from.map(_.toString).mkString(" |> "))
//        sb.append(")")
//    }
//
////    alias.foreach { x =>
////      sb.append(" AS ")
////      sb.append(x.substring(TableName.SodaFountainTableNamePrefixSubStringIndex))
////    }
//
//    sb.append(" ON ")
//    sb.append(on.toString)
//    sb.toString
//  }
}

case class InnerJoin[ColumnId, Type](from: From[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = InnerJoinType
}

case class LeftOuterJoin[ColumnId, Type](from: From[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = LeftOuterJoinType
}

case class RightOuterJoin[ColumnId, Type](from: From[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = RightOuterJoinType
}

case class FullOuterJoin[ColumnId, Type](from: From[ColumnId, Type], on: CoreExpr[ColumnId, Type]) extends Join[ColumnId, Type] {
  val typ: JoinType = FullOuterJoinType
}

object Join {

  def apply[ColumnId, Type](joinType: JoinType, from: From[ColumnId, Type], on: CoreExpr[ColumnId, Type]): Join[ColumnId, Type] = {
    joinType match {
      case InnerJoinType => InnerJoin(from, on)
      case LeftOuterJoinType => LeftOuterJoin(from, on)
      case RightOuterJoinType => RightOuterJoin(from, on)
      case FullOuterJoinType => FullOuterJoin(from, on)
    }
  }
}
