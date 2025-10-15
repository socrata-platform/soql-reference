package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.collection._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2

class LimitIfUnlimited[MT <: MetaTypes] private (labelProvider: LabelProvider) extends StatementUniverse[MT] {
  def isLimited(stmt: Statement): Boolean =
    stmt match {
      case CombinedTables(TableFunc.Union | TableFunc.UnionAll, left, right) =>
        isLimited(left) && isLimited(right)

      case CombinedTables(TableFunc.Intersect | TableFunc.IntersectAll, left, right) =>
        isLimited(left) || isLimited(right)

      case CombinedTables(TableFunc.Minus | TableFunc.MinusAll, left, right) =>
        isLimited(left)

      case Values(_, _) =>
        // this one's kinda weird, but I've decided to go with the
        // intrepretation "it is limited by virtue of having all its
        // rows explicitly specified in the query".
        true

      case cte: CTE =>
        isLimited(cte.useQuery)

      case sel: Select =>
        sel.limit.isDefined
    }

  def rewriteStatement(stmt: Statement, desiredLimit: BigInt): Statement = {
    if(isLimited(stmt)) {
      stmt
    } else {
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
            Some(desiredLimit),
            None,
            None,
            Set.empty
          )

        case cte: CTE =>
          cte.copy(useQuery = rewriteStatement(cte.useQuery, desiredLimit))

        case sel: Select =>
          sel.copy(limit = Some(desiredLimit))
      }
    }
  }
}

object LimitIfUnlimited {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, statement: Statement[MT], limit: NonNegativeBigInt): Statement[MT] = {
    new LimitIfUnlimited(labelProvider).rewriteStatement(statement, limit.underlying)
  }
}
