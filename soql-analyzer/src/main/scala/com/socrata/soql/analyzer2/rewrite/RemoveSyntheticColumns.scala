package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveSyntheticColumns[MT <: MetaTypes] private (labelProvider: LabelProvider) extends StatementUniverse[MT] {
  def rewriteStatement(stmt: Statement): Statement =
    stmt match {
      case ct@CombinedTables(_op, _left, _right) =>
        if(ct.schema.values.exists(_.isSynthetic)) {
          // there shouldn't ever actually be any synthetic columns to
          // remove here, so this is "just in case".  We'll add a new
          // wrapper select that drops the columns, rather than trying
          // to rewrite left and right in an equivalent way.  Note we
          // don't care about preserving our own labels, as this is
          // the output schema for the analysis.

          val newFrom = FromStatement(ct, labelProvider.tableLabel(), None, None)
          Select(
            Distinctiveness.Indistinct(),
            OrderedMap() ++ ct.schema.iterator.flatMap { case (k, v) =>
              if(v.isSynthetic) {
                None
              } else {
                Some(labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newFrom.label, k, v.typ)(AtomicPositionInfo.Synthetic), v.name, v.hint, isSynthetic = false))
              }
            },
            newFrom,
            None, Nil, None, Nil, None, None, None, Set.empty
          )
        } else {
          ct
        }

      case cte@CTE(_defLabel, _defAlias, _defQuery, _matHint, useQuery) =>
        cte.copy(useQuery = rewriteStatement(useQuery))

      case v@Values(_, _) =>
        v

      case select@Select(Distinctiveness.Indistinct() | Distinctiveness.On(_), selectList, _from, _where, _groupBy, _having, _orderBy, _limit, _offset, _search, _hint) =>
        select.copy(selectList = OrderedMap() ++ selectList.iterator.filterNot { case (k, _) => select.schema(k).isSynthetic })

      case select@Select(Distinctiveness.FullyDistinct(), selectList, _from, _where, _groupBy, _having, orderBy, _limit, _offset, _search, _hint) =>
        if(select.schema.values.exists(_.isSynthetic)) {
          // This also sholdn't ever happen, but we can treat it much
          // the same way we treat combined tables (i.e., wrapping it
          // in a SELECT that drops the undesired columns), with the
          // wrinkle that we have to preserve ordering - but in this
          // case any ORDER BY clauses must also be selected, so we
          // can just ORDER BY the output columns of the inner select.

          val newFrom = FromStatement(select, labelProvider.tableLabel(), None, None)
          Select(
            Distinctiveness.Indistinct(),
            OrderedMap() ++ select.schema.iterator.flatMap { case (k, v) =>
              if(v.isSynthetic) {
                None
              } else {
                Some(labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newFrom.label, k, v.typ)(AtomicPositionInfo.Synthetic), v.name, v.hint, isSynthetic = false))
              }
            },
            newFrom,
            None, Nil, None,
            orderBy.map { originalOrderBy =>
              val (selectedColumn, selectedExpr) = selectList.iterator.find { case (k, v) =>
                v.expr == originalOrderBy.expr
              }.getOrElse {
                throw new Exception("Have a fully DISTINCT query with an ORDER BY which is not selected?")
              }
              OrderBy(
                VirtualColumn(newFrom.label, selectedColumn, selectedExpr.typ)(AtomicPositionInfo.Synthetic),
                ascending = originalOrderBy.ascending,
                nullLast = originalOrderBy.nullLast
              )
            },
            None, None, None, Set.empty
          )
        } else {
          select
        }
    }
}

object RemoveSyntheticColumns {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, stmt: Statement[MT]): Statement[MT] = {
    new RemoveSyntheticColumns[MT](labelProvider).rewriteStatement(stmt)
  }
}
