package com.socrata.soql.typed

import com.socrata.soql.{BasedSoQLAnalysis, SimpleSoQLAnalysis, SoQLAnalysis, typed}
import com.socrata.soql.ast._
import com.socrata.soql.environment.TableName

trait TableSource[ColumnId, Type] {
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): TableSource[NewColumnId, Type]
}

// TODO: can these just extend TableSource[Nothing, Nothing]?
case class TableName[ColumnId, Type](name: String) extends TableSource[ColumnId, Type] {
  override def toString: String = {
    val unPrefixedName = name.substring(com.socrata.soql.environment.TableName.SodaFountainTableNamePrefixSubStringIndex)
    s"@$unPrefixedName"
  }

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): TableName[NewColumnId, Type] = {
    typed.TableName(name)
  }
}
class NoContext[ColumnId, Type] extends TableSource[ColumnId, Type] {
  override def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): TableSource[NewColumnId, Type] = {
    new NoContext[NewColumnId, Type]
  }
}

// TODO: does this From need an alias? what's the point?
case class From[ColumnId, Type](source: TableSource[ColumnId, Type], refs: List[SoQLAnalysis[ColumnId, Type]], alias: Option[String]) {
//  override def toString: String = {
//
//  }

  val isTable = source match {
    case _: TableName[_, _] => true
    case _ => false
  }

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): From[NewColumnId, Type] = {
    def fromMap(c: ColumnId, q: Qualifier): NewColumnId = {
      val tableSourceNameOpt = source match {
        case TableName(name) => Some(name)
        case _ => None
      }
      val qualifierForFrom = q.orElse(tableSourceNameOpt)
      f(c, qualifierForFrom)
    }

    typed.From(source.mapColumnIds(fromMap), refs.map(_.mapColumnIds(fromMap)), alias)
  }
}

sealed trait Join[ColumnId, Type] {
  val from: From[ColumnId, Type]
  val on: CoreExpr[ColumnId, Type]
  val typ: JoinType

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(typ.toString)
    sb.append(" ")
    from match {
      case From(TableName(name), _, _) => name.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix)
      case f =>
        sb.append("(")
        sb.append(f.map(_.toString).mkString(" |> "))
        sb.append(")")
    }
    SimpleSoQLAnalysis.asSoQL(from) match {
      case Some(x) =>
        sb.append(x.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix))
      case None =>
        sb.append("(")
        sb.append(from.map(_.toString).mkString(" |> "))
        sb.append(")")
    }

    alias.foreach { x =>
      sb.append(" AS ")
      sb.append(x.substring(TableName.SodaFountainTableNamePrefixSubStringIndex))
    }

    sb.append(" ON ")
    sb.append(on.toString)
    sb.toString
  }

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): Join[NewColumnId, Type] = {
    typed.Join(typ, from.mapColumnIds(f), on.mapColumnIds(f))
  }
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
