package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2

class AddLimitOffset[RNS, CT, CV] private (labelProvider: LabelProvider) {
  type Statement = analyzer2.Statement[RNS, CT, CV]

  def rewriteStatement(stmt: Statement, desiredLimit: Option[BigInt], desiredOffset: Option[BigInt]): Statement = {
    stmt match {
      case CombinedTables(_, _, _) | Values(_) =>
        // combinedtables and values don't specify an order, so we don't need to preserve an order
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

  def apply[RNS, CT, CV](labelProvider: LabelProvider, statement: Statement[RNS, CT, CV], limit: Option[BigInt], offset: Option[BigInt]): Statement[RNS, CT, CV] = {
    if(limit.isEmpty && offset.isEmpty) {
      statement
    } else {
      require(limit.getOrElse(zero) >= zero)
      require(offset.getOrElse(zero) >= zero)

      new AddLimitOffset(labelProvider).rewriteStatement(statement, limit, offset)
    }
  }
}
