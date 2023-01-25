package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2

class AddLimitOffset[MT <: MetaTypes] private (labelProvider: LabelProvider) extends SoQLAnalyzerUniverse[MT] {
  def rewriteStatement(stmt: Statement, desiredLimit: Option[BigInt], desiredOffset: Option[BigInt]): Statement = {
    stmt match {
      case CombinedTables(_, _, _) | Values(_) =>
        val newTableLabel = labelProvider.tableLabel()
        Select(
          Distinctiveness.Indistinct,
          OrderedMap() ++ stmt.schema.iterator.map { case (colLabel, NameEntry(name, typ)) =>
            labelProvider.columnLabel() -> NamedExpr(Column(newTableLabel, colLabel, typ)(AtomicPositionInfo.None), name)
          },
          FromStatement(stmt, newTableLabel, None, None),
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
  private val zero = BigInt(0)

  def apply[MT <: MetaTypes](labelProvider: LabelProvider, statement: Statement[MT], limit: Option[BigInt], offset: Option[BigInt]): Statement[MT] = {
    if(limit.isEmpty && offset.isEmpty) {
      statement
    } else {
      require(limit.getOrElse(zero) >= zero)
      require(offset.getOrElse(zero) >= zero)

      new AddLimitOffset(labelProvider).rewriteStatement(statement, limit, offset)
    }
  }
}
