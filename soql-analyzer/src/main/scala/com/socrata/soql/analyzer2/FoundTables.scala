package com.socrata.soql.analyzer2

import com.socrata.soql.ast
import com.socrata.soql.environment.{ResourceName, HoleName}
import com.socrata.soql.BinaryTree

case class FoundTables[ResourceNameScope, +ColumnType](
  tableMap: TableMap[ResourceNameScope, ColumnType],
  initialScope: ResourceNameScope,
  initialQuery: FoundTables.Query
) {
  val knownUserParameters: Map[CanonicalName, Map[HoleName, ColumnType]] =
    tableMap.descriptions.foldLeft(Map.empty[CanonicalName, Map[HoleName, ColumnType]]) { (acc, desc) =>
      desc match {
        case _ : ParsedTableDescription.Dataset[_] | _ : ParsedTableDescription.TableFunction[_, _] => acc
        case q: ParsedTableDescription.Query[_, ColumnType] => acc + (q.canonicalName -> q.parameters)
      }
    }

  // This lets you convert resource scope names to a simplified form
  // if your resource scope names in one location have semantic
  // meaning that you don't care to serialize.  You also get a map
  // from the meaningless name to the meaningful one so if you want to
  // (for example) translate an error from the analyzer back into the
  // meaningful form, you can do that.
  lazy val (withSimplifiedScopes, simplifiedScopeMap) = locally {
    val (newMap, newToOld, oldToNew) = tableMap.rewriteScopes(initialScope)

    val newFT = FoundTables(
      newMap,
      oldToNew(initialScope),
      initialQuery
    )

    (newMap, newToOld)
  }
}

object FoundTables {
  sealed abstract class Query
  case class Saved(name: ResourceName) extends Query
  case class InContext(parent: ResourceName, soql: BinaryTree[ast.Select]) extends Query
  case class InContextImpersonatingSaved(parent: ResourceName, soql: BinaryTree[ast.Select], fake: CanonicalName) extends Query
  case class Standalone(soql: BinaryTree[ast.Select]) extends Query
}

