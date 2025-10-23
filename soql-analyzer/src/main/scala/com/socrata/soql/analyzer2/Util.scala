package com.socrata.soql.analyzer2

import com.socrata.soql.ast
import com.socrata.soql.environment.{ColumnName, ResourceName, ScopedResourceName, TableName}
import com.socrata.soql.{BinaryTree, Leaf, Compound}

import SoQLAnalyzerError.TypecheckError.NoSuchColumn

private[analyzer2] object Util {
  def walkParsed[MT <: MetaTypes](acc: Set[types.ScopedResourceName[MT]], scope: types.ResourceNameScope[MT], parsed: BinaryTree[ast.Select]): Set[types.ScopedResourceName[MT]] =
    parsed match {
      case Leaf(select) => walkParsed(acc, scope, select)
      case Compound(_, left, right) => walkParsed(walkParsed(acc, scope, left), scope, right)
    }

  def walkParsed[MT <: MetaTypes](acc0: Set[types.ScopedResourceName[MT]], scope: types.ResourceNameScope[MT], parsed: ast.Select): Set[types.ScopedResourceName[MT]] = {
    val acc = parsed.from match {
      case Some(tableName) => acc0 + ScopedResourceName(scope, ResourceName(tableName.nameWithoutPrefix))
      case None => acc0
    }
    parsed.joins.foldLeft(acc) { (acc, join) =>
      join.from match {
        case ast.JoinTable(tableName) => acc + ScopedResourceName(scope, ResourceName(tableName.nameWithoutPrefix))
        case ast.JoinQuery(selects, _) => walkParsed(acc, scope, selects)
        case ast.JoinFunc(tableName, _) => acc + ScopedResourceName(scope, ResourceName(tableName.nameWithoutPrefix))
      }
    }
  }

  def isSpecialTableName(scopedName: ScopedResourceName[_]): Boolean = {
    val ScopedResourceName(_, name) = scopedName
    val prefixedName = TableName.SodaFountainPrefix + name.caseFolded
    TableName.reservedNames.contains(prefixedName)
  }

  private def closeEnough(a: String, b: String): Boolean =
    Levenshtein(a, b) < 3

  private def closeEnough(a: Option[String], b: Option[String]): Boolean =
    (a, b) match {
      case (None, None) => true
      case (Some(a), Some(b)) => closeEnough(a, b)
      case _ => false
    }

  def possibilitiesFor(env: Environment[_], additionalNames: Iterator[ColumnName], qual: Option[ResourceName], name: ColumnName): Seq[NoSuchColumn.ColumnCandidate] = {
    val candidates =
      for {
        (tableName, columnName) <- additionalNames.map((None, _)) ++ env.contents
        if closeEnough(qual.map(_.caseFolded), tableName.map(_.caseFolded)) || // qualifiers match, or
           (qual.isEmpty && tableName.isDefined)                               // the user provided no qualifier but the candidate has one
        if closeEnough(columnName.caseFolded, name.caseFolded)
      } yield NoSuchColumn.ColumnCandidate(tableName, columnName)

    candidates.toVector.distinct
  }

  def mergeColumnSet[T](
    a: Map[AutoTableLabel, Set[ColumnLabel[T]]],
    b: Map[AutoTableLabel, Set[ColumnLabel[T]]]
  ): Map[AutoTableLabel, Set[ColumnLabel[T]]] = {
    if(a.size < b.size) {
      mergeColumnSet(b, a)
    } else {
      b.foldLeft(a) { case (acc, (table, cols)) =>
        val newCols = acc.get(table) match {
          case None => cols
          case Some(knownCols) => knownCols ++ cols
        }
        acc + (table -> newCols)
      }
    }
  }

  def augmentColumnSet[T](
    a: Map[AutoTableLabel, Set[ColumnLabel[T]]],
    bs: Iterator[(AutoTableLabel, ColumnLabel[T])]
  ): Map[AutoTableLabel, Set[ColumnLabel[T]]] = {
    bs.foldLeft(a) { case (acc, (table, col)) =>
      val newCols = acc.get(table) match {
        case None => Set(col)
        case Some(cols) => cols + col
      }
      acc + (table -> newCols)
    }
  }
}
