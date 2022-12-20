package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class RemoveUnusedColumns[RNS, CT, CV] private (columnReferences: Map[TableLabel, Set[ColumnLabel]]) {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]

  // myLabel being "None" means "keep all of my output columns,
  // whether or not they appear to be used".  This is for both the
  // top-level (where of course all the columns will be used by
  // whatever's running the query) as well as inside CombinedTables,
  // where the operation combining the tables will care about
  // apparently-unused columns.
  def rewriteStatement(stmt: Statement, myLabel: Option[TableLabel]): (Statement, Boolean) = {
    stmt match {
      case CombinedTables(op, left, right) =>
        val (newLeft, removedAnythingLeft) = rewriteStatement(left, None)
        val (newRight, removedAnythingRight) = rewriteStatement(right, None)
        (CombinedTables(op, newLeft, newRight), removedAnythingLeft || removedAnythingRight)

      case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        val (newDefQuery, removedAnythingDef) = rewriteStatement(defQuery, Some(defLabel))
        val (newUseQuery, removedAnythingUse) = rewriteStatement(useQuery, myLabel)
        (CTE(defLabel, defAlias, newDefQuery, materializedHint, newUseQuery), removedAnythingDef || removedAnythingUse)

      case v@Values(_) =>
        (v, false)

      case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val newSelectList = (myLabel, distinctiveness) match {
          case (_, Distinctiveness.FullyDistinct) | (None, _) =>
            // need to keep all my columns
            selectList
          case (Some(tl), _) =>
            val wantedColumns = columnReferences.getOrElse(tl, Set.empty)
            selectList.filter { case (cl, _) => wantedColumns(cl) }
        }
        val removedAnythingSelectList = selectList.size != newSelectList.size

        val (newFrom, removedAnythingFrom) = rewriteFrom(from)
        val candidate = stmt.copy(selectList = newSelectList, from = newFrom)
        if(candidate.isAggregated != stmt.isAggregated) {
          // this is a super-extreme edge case, but consider
          //   select x.x from (select count(*), 1 as x from whatever) as x
          // Doing a naive "remove unused columns" would result in
          //  select x.x from (select 1 as x from whatever) as x
          // ..which changes the semantics of that inner query.  So, if removing
          // columns from our select list changed whether or not we're aggregated,
          // keep our column-list as-is.  This should hopefully basically never
          // happen in practice.
          (stmt.copy(from = newFrom), removedAnythingFrom)
        } else {
          (candidate, removedAnythingSelectList || removedAnythingFrom)
        }
    }
  }

  def rewriteFrom(from: From): (From, Boolean) = {
    from.reduceMap[Boolean, RNS, CT, CV](
      rewriteAtomicFrom(_).swap,
      { (removedAnythingLeft, joinType, lateral, left, right, on) =>
        val (newRight, removedAnythingRight) = rewriteAtomicFrom(right)
        (removedAnythingLeft || removedAnythingRight, Join(joinType, lateral, left, newRight, on))
      }
    ).swap
  }

  def rewriteAtomicFrom(from: AtomicFrom): (AtomicFrom, Boolean) = {
    from match {
      case ft: FromTable => (ft, false)
      case fsr: FromSingleRow => (fsr, false)
      case FromStatement(stmt, label, resourceName, alias) =>
        val (newStmt, removedAnything) = rewriteStatement(stmt, Some(label))
        (FromStatement(newStmt, label, resourceName, alias), removedAnything)
    }
  }
}

/** Remove columns that are not useful from inner selects.
  * SelectListReferences must not be present (this is unchecked!!). */
object RemoveUnusedColumns {
  def apply[RNS, CT, CV](stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    val (newStmt, removedAnything) =
      new RemoveUnusedColumns[RNS, CT, CV](stmt.columnReferences).rewriteStatement(stmt, None)
    if(removedAnything) {
      this(newStmt)
    } else {
      newStmt
    }
  }
}
