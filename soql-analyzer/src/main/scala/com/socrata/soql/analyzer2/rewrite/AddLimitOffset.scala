package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2

class AddLimitOffset[MT <: MetaTypes] private (labelProvider: LabelProvider) extends StatementUniverse[MT] {
  def rewriteStatement(stmt: Statement, desiredLimit: Option[BigInt], desiredOffset: Option[BigInt]): Statement = {
    stmt match {
      case CombinedTables(_, _, _) | Values(_, _) =>
        val newTableLabel = labelProvider.tableLabel()
        Select(
          Distinctiveness.Indistinct(),
          OrderedMap() ++ stmt.schema.iterator.map { case (colLabel, Statement.SchemaEntry(name, typ, hint, isSynthetic)) =>
            labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newTableLabel, colLabel, typ)(AtomicPositionInfo.Synthetic), name, hint = None, isSynthetic = isSynthetic)
          },
          FromStatement(stmt, newTableLabel, None, None, None),
          None,
          Nil,
          None,
          Nil,
          desiredLimit,
          desiredOffset,
          None,
          Set.empty
        )

      case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        cte.copy(useQuery = rewriteStatement(useQuery, desiredLimit, desiredOffset))

      case sel@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, oldLimit, oldOffset, search, hint) =>
        val (newLimit, newOffset) = Merger.combineLimits(oldLimit, oldOffset, desiredLimit, desiredOffset)
        sel.copy(limit = newLimit, offset = newOffset)
    }
  }
}

object AddLimitOffset {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, statement: Statement[MT], limit: Option[NonNegativeBigInt], offset: Option[NonNegativeBigInt]): Statement[MT] = {
    if(limit.isEmpty && offset.isEmpty) {
      statement
    } else {
      new AddLimitOffset(labelProvider).rewriteStatement(statement, limit.map(_.underlying), offset.map(_.underlying))
    }
  }
}
