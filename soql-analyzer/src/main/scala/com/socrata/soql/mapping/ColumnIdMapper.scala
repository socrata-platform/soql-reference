package com.socrata.soql.mapping

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.typed.Qualifier
import com.socrata.soql.{BinaryTree, Compound, PipeQuery, SoQLAnalysis}

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
        val prev = nl.previous
        val prevQColumnIdToQColumnIdMap = prev.selection.foldLeft(qColumnIdNewColumnIdMap) { (acc, selCol) =>
          val (colName, expr) = selCol
          acc + (qColumnNameToQColumnId(None, colName) -> columnNameToNewColumnId(colName))
        }
        val newQColumnIdToQColumnIdMap = qColumnIdNewColumnIdMap ++ prevQColumnIdToQColumnIdMap
        val nr = r.asLeaf.mapColumnIds(newQColumnIdToQColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        PipeQuery(nl, nr)
      case Compound(op, l, r) =>
        val nl = mapColumnIds(l)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        val nr = mapColumnIds(r)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        Compound(op, nl, nr)
      case s =>
        val ana = s.asLeaf
        val nana = ana.mapColumnIds(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
        nana
    }
  }
}
