package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class SelectListReferences[RNS, CT, CV] private () {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]

  object Use {
    def rewriteStatement(stmt: Statement): Statement = {
      stmt match {
        case CombinedTables(op, left, right) =>
          CombinedTables(op, rewriteStatement(left), rewriteStatement(right))

        case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
          CTE(defLabel, defAlias, rewriteStatement(defQuery), materializedHint, rewriteStatement(useQuery))

        case v@Values(_) =>
          v

        case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
          val selectListIndices = selectList.valuesIterator.map(_.expr).toVector.zipWithIndex.reverseIterator.toMap

          def numericateExpr(e: Expr[CT, CV]): Expr[CT, CV] = {
            e match {
              case c: Column[CT] =>
                c // don't bother rewriting column references
              case e =>
                selectListIndices.get(e) match {
                  case Some(idx) => SelectListReference(idx + 1, e.isAggregated, e.isWindowed, e.typ)(e.position.asAtomic)
                  case None => e
                }
            }
          }

          stmt.copy(
            distinctiveness = distinctiveness match {
              case Distinctiveness.Indistinct | Distinctiveness.FullyDistinct => distinctiveness
              case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(numericateExpr))
            },
            from = rewriteFrom(from),
            groupBy = groupBy.map(numericateExpr),
            orderBy = orderBy.map { ob => ob.copy(expr = numericateExpr(ob.expr)) }
          )
      }
    }

    def rewriteFrom(from: From): From = {
      from.map[RNS, CT, CV](
        rewriteAtomicFrom(_),
        { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right), on) }
      )
    }

    def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
      from match {
        case ft: FromTable => ft
        case fsr: FromSingleRow => fsr
        case fs@FromStatement(stmt, label, resourceName, alias) =>
          fs.copy(statement = rewriteStatement(stmt))
      }
    }
  }

  object Unuse {
    def rewriteStatement(stmt: Statement): Statement = {
      stmt match {
        case CombinedTables(op, left, right) =>
          CombinedTables(op, rewriteStatement(left), rewriteStatement(right))

        case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
          CTE(defLabel, defAlias, rewriteStatement(defQuery), materializedHint, rewriteStatement(useQuery))

        case v@Values(_) =>
          v

        case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
          val selectListIndices = selectList.valuesIterator.map(_.expr).toVector

          def unnumericateExpr(e: Expr[CT, CV]): Expr[CT, CV] = {
            e match {
              case r@SelectListReference(idxPlusOne, _, _, _) =>
                selectListIndices(idxPlusOne - 1).reposition(r.position.logicalPosition)
              case other =>
                other
            }
          }

          stmt.copy(
            distinctiveness = distinctiveness match {
              case Distinctiveness.Indistinct | Distinctiveness.FullyDistinct => distinctiveness
              case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(unnumericateExpr))
            },
            from = rewriteFrom(from),
            groupBy = groupBy.map(unnumericateExpr),
            orderBy = orderBy.map { ob => ob.copy(expr = unnumericateExpr(ob.expr)) }
          )
      }
    }

    def rewriteFrom(from: From): From = {
      from.map[RNS, CT, CV](
        rewriteAtomicFrom(_),
        { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right), on) }
      )
    }

    def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
      from match {
        case ft: FromTable => ft
        case fsr: FromSingleRow => fsr
        case fs@FromStatement(stmt, label, resourceName, alias) =>
          fs.copy(statement = rewriteStatement(stmt))
      }
    }
  }
}

/** Rewrite the given statement to either use or stop using
  * select-list references in DISTINCT ON, GROUP BY, and ORDER BY
  * clauses.
  *
  * e.g., this will rewrite a Statement that corresponds to "select
  * x+1, count(*) group by x+1 order by count(*)" to one that
  * corresponds to "select x+1, count(*) group by 1 order by 2"
  */
object SelectListReferences {
  def use[RNS, CT, CV](stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    new SelectListReferences[RNS, CT, CV]().Use.rewriteStatement(stmt)
  }

  def unuse[RNS, CT, CV](stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    new SelectListReferences[RNS, CT, CV]().Unuse.rewriteStatement(stmt)
  }
}
