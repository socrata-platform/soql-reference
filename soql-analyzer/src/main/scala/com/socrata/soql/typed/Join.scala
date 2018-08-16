package com.socrata.soql.typed

import com.socrata.soql.{BasedSoQLAnalysis, SimpleSoQLAnalysis, SoQLAnalysis, typed}
import com.socrata.soql.ast._
import com.socrata.soql.environment.{ColumnName, TableName}

trait TableSource[ColumnId, Type] {
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): TableSource[NewColumnId, Type]
}

// TODO: can these just extend TableSource[Nothing, Nothing]?
case class TableName[ColumnId, Type](name: String) extends TableSource[ColumnId, Type] {
  override def toString: String = {
    name.replaceFirst(com.socrata.soql.environment.TableName.SodaFountainTableNamePrefix, com.socrata.soql.environment.TableName.Prefix)
  }

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): TableName[NewColumnId, Type] = {
    typed.TableName(name)
  }
}

class NoContext[ColumnId, Type] extends TableSource[ColumnId, Type] {
  override def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): TableSource[NewColumnId, Type] = {
    new NoContext[NewColumnId, Type]
  }
  override def toString: String = ""
}

// TODO: does this From need an alias? what's the point?
case class From[ColumnId, Type](source: TableSource[ColumnId, Type], refs: List[SoQLAnalysis[ColumnId, Type]], alias: Option[String]) {
  override def toString: String = {
    source.toString + refs.mkString(" |> ", " |> ", "") + alias.map(a => s" as $a")
  }

  def lastSelection = {
    refs.lastOption.map(_.selection).orElse(source match {
      case bs: BasedSoQLAnalysis[_, _] => Some(bs.selection)
      case _ => None
    })
  }

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

  def sourceTableName = source match {
    case TableName(name) => Some(name)
    case _ => None
  }

  def mapColumnIds[NewColumnId](qColumnIdNewColumnIdMap: Map[(ColumnId, Qualifier), NewColumnId],
                                qColumnNameToQColumnId: (Qualifier, ColumnName) => (ColumnId, Qualifier),
                                columnNameToNewColumnId: ColumnName => NewColumnId,
                                columnIdToNewColumnId: ColumnId => NewColumnId): From[NewColumnId, Type] = {
    val initialJoinMap = qColumnIdNewColumnIdMap.foldLeft(Map.empty[(ColumnId, Qualifier), NewColumnId]) { (acc, kv) =>
      val ((cid, qual), ncid) = kv
      if (qual == sourceTableName) acc + ((cid, None) -> ncid)
      else acc
    }

    val bsMapped = source match {
      case bs: BasedSoQLAnalysis[ColumnId, Type] =>
        Some(bs.mapColumnIds(initialJoinMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId))
      case _ => None
    }

    val mappedRefs = refs.foldLeft(bsMapped.map(_.decontextualized).toList) { (convertedAnalyses, analysis) =>
      val joinMap = convertedAnalyses.lastOption match {
        case Some(prevAnalysis) =>
          prevAnalysis.selection.foldLeft(qColumnIdNewColumnIdMap) { (acc, selCol) =>
            val (colName, _) = selCol
            acc + (qColumnNameToQColumnId(None, colName) -> columnNameToNewColumnId(colName))
          }
        case None => initialJoinMap
      }
      val mapped = analysis.mapColumnIds(joinMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
      convertedAnalyses :+ mapped
    }

    bsMapped match {
      case None => typed.From(sourceTableName.map(typed.TableName(_)).getOrElse(new NoContext), mappedRefs, alias)
      case Some(bs) => typed.From(bs, mappedRefs.tail, alias) // mappedRefs.head is actually decontextualized bs
    }
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
    sb.append(from.toString)
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
