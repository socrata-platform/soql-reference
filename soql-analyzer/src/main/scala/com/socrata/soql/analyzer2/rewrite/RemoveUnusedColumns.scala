package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveUnusedColumns[MT <: MetaTypes] private (columnReferences: Map[types.AutoTableLabel[MT], Set[types.ColumnLabel[MT]]]) extends StatementUniverse[MT] {
  // myLabel being "None" means "keep all of my output columns,
  // whether or not they appear to be used".  This is for both the
  // top-level (where of course all the columns will be used by
  // whatever's running the query) as well as inside CombinedTables,
  // where the operation combining the tables will care about
  // apparently-unused columns.
  def rewriteStatement(stmt: Statement, myLabel: Option[AutoTableLabel]): (Statement, Boolean) = {
    stmt match {
      case CombinedTables(op, left, right) =>
        val (newLeft, removedAnythingLeft) = rewriteStatement(left, None)
        val (newRight, removedAnythingRight) = rewriteStatement(right, None)
        (CombinedTables(op, newLeft, newRight), removedAnythingLeft || removedAnythingRight)

      case CTE(defns, useQuery) =>
        val (newDefnsRev, removedAnythingDef) = defns.iterator.foldLeft((List.empty[(AutoTableLabel, CTE.Definition[MT])], false)) { case ((newDefnsRev, removedAnythingDef), (defLabel, defn)) =>
          val (newQuery, removed)  = rewriteStatement(defn.query, Some(defLabel))
          ((defLabel -> defn.copy(query = newQuery)) :: newDefnsRev, removed || removedAnythingDef)
        }
        val newDefns = OrderedMap() ++ newDefnsRev.reverse
        val (newUseQuery, removedAnythingUse) = rewriteStatement(useQuery, myLabel)
        (CTE(newDefns, newUseQuery), removedAnythingDef || removedAnythingUse)

      case v@Values(_, _) =>
        (v, false)

      case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val newSelectList = (myLabel, distinctiveness, search) match {
          case (None, _, _) | (_, Distinctiveness.FullyDistinct(), _) | (_, _, Some(_)) =>
            // need to keep all my columns
            selectList
          case (Some(tl), _, _) =>
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
    from.reduceMap[Boolean, MT](
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
      case fc: FromCTE => (fc, false)
      case FromStatement(stmt, label, resourceName, canonicalName, alias) =>
        val (newStmt, removedAnything) = rewriteStatement(stmt, Some(label))
        (FromStatement(newStmt, label, resourceName, canonicalName, alias), removedAnything)
    }
  }
}

/** Remove columns that are not useful from inner selects.
  * SelectListReferences must not be present (this is unchecked!!). */
object RemoveUnusedColumns {
  def apply[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    val (newStmt, removedAnything) =
      new RemoveUnusedColumns[MT](stmt.columnReferences).rewriteStatement(stmt, None)
    if(removedAnything) {
      this(newStmt)
    } else {
      newStmt
    }
  }
}
