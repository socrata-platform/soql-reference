package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class SelectListReferences[MT <: MetaTypes] private () extends StatementUniverse[MT] {
  abstract class Transform {
    def rewriteSelect(select: Select): Select

    def rewriteStatement(stmt: Statement): Statement = {
      stmt match {
        case CombinedTables(op, left, right) =>
          CombinedTables(op, rewriteStatement(left), rewriteStatement(right))

        case CTE(defns, useQuery) =>
          val newDefns = defns.withValuesMapped { defn => defn.copy(query = rewriteStatement(defn.query)) }
          CTE(newDefns, rewriteStatement(useQuery))

        case v@Values(_, _) =>
          v

        case select: Select =>
          rewriteSelect(select)
      }
    }

    def rewriteFrom(from: From): From = {
      from.map[MT](
        rewriteAtomicFrom(_),
        { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right), on) }
      )
    }

    def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom = {
      from match {
        case ft: FromTable => ft
        case fsr: FromSingleRow => fsr
        case fc: FromCTE => fc
        case fs@FromStatement(stmt, label, resourceName, canonicalName, alias) =>
          fs.copy(statement = rewriteStatement(stmt))
      }
    }
  }

  object Use extends Transform {
    override def rewriteSelect(select: Select): Select = {
      val Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) = select

      val selectListIndices = selectList.valuesIterator.map(_.expr).toVector.zipWithIndex.reverseIterator.toMap

      def numericateExpr(e: Expr): Expr = {
        e match {
          case c: Column =>
            c // don't bother rewriting column references
          case e =>
            selectListIndices.get(e) match {
              case Some(idx) => SelectListReference(idx + 1, e.isAggregated, e.isWindowed, e.typ)(e.position.asAtomic)
              case None => e
            }
        }
      }

      select.copy(
        distinctiveness = distinctiveness match {
          case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(numericateExpr))
          case x@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => x
        },
        from = rewriteFrom(from),
        groupBy = groupBy.map(numericateExpr),
        orderBy = orderBy.map { ob => ob.copy(expr = numericateExpr(ob.expr)) }
      )
    }
  }

  object Unuse extends Transform {
    override def rewriteSelect(select: Select): Select = {
      val Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) = select

      val selectListIndices = selectList.valuesIterator.map(_.expr).toVector

      def unnumericateExpr(e: Expr): Expr = {
        e match {
          case r@SelectListReference(idxPlusOne, _, _, _) =>
            selectListIndices(idxPlusOne - 1).reReference(r.position.reference)
          case other =>
            other
        }
      }

      select.copy(
        distinctiveness = distinctiveness match {
          case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(unnumericateExpr))
          case x@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => x
        },
        from = rewriteFrom(from),
        groupBy = groupBy.map(unnumericateExpr),
        orderBy = orderBy.map { ob => ob.copy(expr = unnumericateExpr(ob.expr)) }
      )
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
  def use[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    new SelectListReferences[MT]().Use.rewriteStatement(stmt)
  }

  def unuse[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    new SelectListReferences[MT]().Unuse.rewriteStatement(stmt)
  }
}
