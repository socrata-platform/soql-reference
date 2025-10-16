package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

private[rewrite] class RemoveOutputColumns[MT <: MetaTypes] (labelProvider: LabelProvider, toRemove: Statement.SchemaEntry[MT] => Boolean) extends StatementUniverse[MT] {
  def rewriteStatement(stmt: Statement): Statement =
    stmt match {
      case ct@CombinedTables(_op, _left, _right) =>
        if(ct.schema.values.exists(toRemove)) {
          val newFrom = FromStatement(ct, labelProvider.tableLabel(), None, None, None)
          Select(
            Distinctiveness.Indistinct(),
            OrderedMap() ++ ct.schema.iterator.flatMap { case (k, v) =>
              if(toRemove(v)) {
                None
              } else {
                Some(labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newFrom.label, k, v.typ)(AtomicPositionInfo.Synthetic), v.name, v.hint, isSynthetic = v.isSynthetic))
              }
            },
            newFrom,
            None, Nil, None, Nil, None, None, None, Set.empty
          )
        } else {
          ct
        }

      case cte@CTE(_defns, useQuery) =>
        cte.copy(useQuery = rewriteStatement(useQuery))

      case v@Values(_, _) =>
        v

      case select@Select(Distinctiveness.Indistinct() | Distinctiveness.On(_), selectList, _from, _where, _groupBy, _having, _orderBy, _limit, _offset, _search, _hint) =>
        select.copy(selectList = OrderedMap() ++ selectList.iterator.filterNot { case (k, _) => toRemove(select.schema(k)) })

      case select@Select(Distinctiveness.FullyDistinct(), selectList, _from, _where, _groupBy, _having, orderBy, _limit, _offset, _search, _hint) =>
        if(select.schema.values.exists(toRemove)) {
          val newFrom = FromStatement(select, labelProvider.tableLabel(), None, None, None)
          Select(
            Distinctiveness.Indistinct(),
            OrderedMap() ++ select.schema.iterator.flatMap { case (k, v) =>
              if(toRemove(v)) {
                None
              } else {
                Some(labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](newFrom.label, k, v.typ)(AtomicPositionInfo.Synthetic), v.name, v.hint, isSynthetic = v.isSynthetic))
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
