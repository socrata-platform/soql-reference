package com.socrata.soql.analyzer2

import com.socrata.soql.ast
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.{BinaryTree, Leaf, Compound}

private[analyzer2] object Util {
  def walkParsed[MT <: MetaTypes](acc: Set[ScopedResourceName[MT#ResourceNameScope]], scope: MT#ResourceNameScope, parsed: BinaryTree[ast.Select]): Set[ScopedResourceName[MT#ResourceNameScope]] =
    parsed match {
      case Leaf(select) => walkParsed(acc, scope, select)
      case Compound(_, left, right) => walkParsed(walkParsed(acc, scope, left), scope, right)
    }

  def walkParsed[MT <: MetaTypes](acc0: Set[ScopedResourceName[MT#ResourceNameScope]], scope: MT#ResourceNameScope, parsed: ast.Select): Set[ScopedResourceName[MT#ResourceNameScope]] = {
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
}
