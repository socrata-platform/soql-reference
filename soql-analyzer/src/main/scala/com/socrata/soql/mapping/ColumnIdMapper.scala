package com.socrata.soql.mapping

import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.exceptions.RightSideOfChainQueryMustBeLeaf
import com.socrata.soql.typed.Qualifier
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, SoQLAnalysis}

import scala.util.parsing.input.NoPosition

object ColumnIdMapper {
  def mapColumnIds[ColumnId, Type, NewColumnId](bt: BinaryTree[SoQLAnalysis[ColumnId, Type]])
                                               (qColumnIdNewColumnIdMap: Map[(ColumnId, Qualifier), NewColumnId],
                                                qColumnNameToQColumnId: (Qualifier, ColumnName) => (ColumnId, Qualifier),
                                                columnNameToNewColumnId: ColumnName => NewColumnId,
                                                columnIdToNewColumnId: ColumnId => NewColumnId):
  BinaryTree[SoQLAnalysis[NewColumnId, Type]] = {
    bt match {
      case PipeQuery(l, r) =>
        val nl = mapColumnIds(l)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        val prev = nl.outputSchema.leaf
        val prevAlias = r.outputSchema.leaf.from match {
          case Some(TableName(TableName.This, alias@Some(_), _)) =>
            alias
          case _ =>
            None
        }
        val prevQColumnIdToQColumnIdMap = prev.selection.foldLeft(qColumnIdNewColumnIdMap) { (acc, selCol) =>
          val (colName, expr) = selCol
          acc + (qColumnNameToQColumnId(prevAlias, colName) -> columnNameToNewColumnId(colName))
        }
        val newQColumnIdToQColumnIdMap = qColumnIdNewColumnIdMap ++ prevQColumnIdToQColumnIdMap
        val rightLeaf = r.asLeaf.getOrElse(throw RightSideOfChainQueryMustBeLeaf(NoPosition))
        val nr = rightLeaf.mapColumnIds(newQColumnIdToQColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        PipeQuery(nl, Leaf(nr))
      case Compound(op, l, r) =>
        val nl = mapColumnIds(l)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        val nr = mapColumnIds(r)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        Compound(op, nl, nr)
      case Leaf(ana) =>
        val nana = ana.mapColumnIds(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        Leaf(nana)
    }
  }
}
