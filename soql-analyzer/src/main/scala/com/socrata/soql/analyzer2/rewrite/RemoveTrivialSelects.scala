package com.socrata.soql.analyzer2.rewrite

import scala.collection.compat._

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveTrivialSelects[MT <: MetaTypes] private () extends StatementUniverse[MT] {
  // A "trivial select" is a select of the form:
  //   select column_ref, ... from atomicfrom
  // without any other modifiers and where all the column_refs point
  // into the atomicfrom.

  object TrivialSelect {
    def unapply(stmt: Statement): Option[(OrderedMap[AutoColumnLabel, Column], AtomicFrom, Boolean)] = {
      stmt match {
        case Select(Distinctiveness.Indistinct(), selectList, from: AtomicFrom, None, Nil, None, Nil, None, None, None, hints) if hints.isEmpty =>
          var exprs = OrderedMap[AutoColumnLabel, Column]()

          for((label, namedExpr) <- selectList) {
            namedExpr.expr match {
              case c: Column if c.table == from.label => exprs += label -> c
              case _ => return None
            }
          }
          assert(exprs.size == selectList.size)

          // first a trivial check to see if we're producing a
          // different number of columns than were in our FROM.
          val fromSchema = from.schema
          var schemaChanged = exprs.size != fromSchema.size
          if(!schemaChanged) {
            // now a less-trivial check to see if we've changed the
            // input schema more subtly.
            schemaChanged = exprs.values.lazyZip(fromSchema).exists { case (columnExpr, From.SchemaEntry(sourceTable, sourceColumn, _typ, _isSynthetic)) =>
              columnExpr.table != sourceTable || columnExpr.column != sourceColumn
            }
          }

          Some((exprs, from, schemaChanged))
        case _ =>
          None
      }
    }
  }

  type ColumnMap = Map[VirtualColumn, Expr]

  def rewriteStatement(stmt: Statement, cm: ColumnMap): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left, cm), rewriteStatement(right, cm))

      case CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        val newDefQuery = rewriteStatement(defQuery, cm)
        val newUseQuery = rewriteStatement(useQuery, cm)
        CTE(defLabel, defAlias, newDefQuery, materializedHint, newUseQuery)

      case v@Values(_, _) =>
        v

      case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val (newFrom, columnMap) = rewriteFrom(from, cm, search.isEmpty)

        val result = Select(
          rewriteDistinctiveness(distinctiveness, columnMap),
          OrderedMap() ++ selectList.iterator.map { case (label, NamedExpr(expr, name, isSynthetic)) =>
            (label, NamedExpr(rewriteExpr(expr, columnMap), name, isSynthetic = isSynthetic))
          },
          newFrom,
          where.map(rewriteExpr(_, columnMap)),
          groupBy.map(rewriteExpr(_, columnMap)),
          having.map(rewriteExpr(_, columnMap)),
          orderBy.map(rewriteOrderBy(_, columnMap)),
          limit,
          offset,
          search,
          hint
        )

        result
    }
  }

  def rewriteDistinctiveness(d: Distinctiveness, cm: ColumnMap): Distinctiveness = {
    d match {
      case noSubExprs@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => noSubExprs
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(rewriteExpr(_, cm)))
    }
  }

  def rewriteExpr(e: Expr, cm: ColumnMap): Expr = {
    e match {
      case vc: VirtualColumn => cm.getOrElse(vc, vc)
      case otherAtomic: AtomicExpr => otherAtomic
      case fc@FunctionCall(func, args) =>
        FunctionCall(func, args.map(rewriteExpr(_, cm)))(fc.position)
      case afc@AggregateFunctionCall(func, args, distinct, filter) =>
        AggregateFunctionCall(func, args.map(rewriteExpr(_, cm)), distinct, filter.map(rewriteExpr(_, cm)))(afc.position)
      case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
        WindowedFunctionCall(
          func,
          args.map(rewriteExpr(_, cm)),
          filter.map(rewriteExpr(_, cm)),
          partitionBy.map(rewriteExpr(_, cm)),
          orderBy.map(rewriteOrderBy(_, cm)),
          frame
        )(wfc.position)
    }
  }

  def rewriteOrderBy(ob: OrderBy, cm: ColumnMap): OrderBy = {
    ob.copy(expr = rewriteExpr(ob.expr, cm))
  }

  def rewriteFrom(from: From, cm: ColumnMap, permitSchemaChange: Boolean): (From, ColumnMap) = {
    from.reduceMap[ColumnMap, MT](
      rewriteAtomicFrom(_, cm, permitSchemaChange).swap,
      { (cm, joinType, lateral, left, right, on) =>
        // need to thread the incrementally-built columnmap through to
        // support lateral joins (if the join is lateral, "right"
        // might refer to output columns from "left" and hence need
        // rewriting)
        val (newRight, newCm) = rewriteAtomicFrom(right, cm, permitSchemaChange)
        (newCm, Join(joinType, lateral, left, newRight, rewriteExpr(on, newCm)))
      }
    ).swap
  }

  def rewriteAtomicFrom(from: AtomicFrom, cm: ColumnMap, permitSchemaChange: Boolean): (AtomicFrom, ColumnMap) = {
    from match {
      case ft: FromTable => (ft, cm)
      case fsr: FromSingleRow => (fsr, cm)
      case fs@FromStatement(stmt, fromLabel, _resourceName, _alias) =>
        rewriteStatement(stmt, cm) match {
          case TrivialSelect(selectList, subFrom, schemaChanges) if permitSchemaChange || !schemaChanges =>
            val newColumnMap =
              cm ++ selectList.iterator.map { case (columnLabel, column) =>
                VirtualColumn(fromLabel, columnLabel, column.typ)(AtomicPositionInfo.Synthetic) -> column
              }
            (subFrom, newColumnMap)
          case other =>
            (fs.copy(statement = other), cm)
        }
    }
  }
}

/** Remove trivial selects, where "trivial" means "only selects
  * columns from some atomic from, with no other action". */
object RemoveTrivialSelects {
  def apply[MT <: MetaTypes](stmt: Statement[MT]): Statement[MT] = {
    new RemoveTrivialSelects[MT]().rewriteStatement(stmt, Map.empty)
  }
}
