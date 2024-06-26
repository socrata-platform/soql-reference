package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class RemoveUnusedOrderBy[MT <: MetaTypes] private () extends StatementUniverse[MT] {
  type RelabelMap = Map[(AutoTableLabel, AutoColumnLabel), (DatabaseColumnName, DatabaseTableName)]

  case class Result(statement: Statement, columnMap: RelabelMap)

  def rewriteStatement(stmt: Statement, callerCaresAboutOrder: Boolean): Statement = {
    stmt match {
      case CombinedTables(op, left, right) =>
        // table ops never preserve ordering
        CombinedTables(op, rewriteStatement(left, false), rewriteStatement(right, false))

      case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        val newUseQuery = rewriteStatement(useQuery, callerCaresAboutOrder)

        cte.copy(
          definitionQuery = rewriteStatement(defQuery, false),
          useQuery = newUseQuery
        )

      case v@Values(_, _) =>
        v

      case select@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val (relabelMap, newFrom) = rewriteFrom(from)

        val newSelect = select.copy(
          from = newFrom,
          orderBy = if(callerCaresAboutOrder || limit.isDefined || offset.isDefined) orderBy else Nil
        )

        if(relabelMap.nonEmpty) {
          new Relabeller(relabelMap).relabel(newSelect)
        } else {
          newSelect
        }
    }
  }

  def rewriteFrom(from: From): (RelabelMap, From) = {
    from.reduceMap[RelabelMap, MT](
      rewriteAtomicFrom(Map.empty, _),
      { (relabelMap, joinType, lateral, left, right, on) =>
        val (newMap, newRight) = rewriteAtomicFrom(relabelMap, right)
        (newMap, Join(joinType, lateral, left, newRight, on))
      }
    )
  }

  def rewriteAtomicFrom(currentMap: RelabelMap, from: AtomicFrom): (RelabelMap, AtomicFrom) = {
    from match {
      case ft: FromTable => (currentMap, ft)
      case fs: FromSingleRow => (currentMap, fs)
      case fs@FromStatement(stmt, label, resourceName, alias) =>
        fs.copy(statement = rewriteStatement(stmt, false)) match {
          // It's possible we've just reduced a statement to "select
          // column1, column2, ... from table" and we want to
          // eliminate the now-unnecessary subselect
          case FromStatement(
            Select(
              Distinctiveness.Indistinct(),
              selectList,
              tbl: FromTable,
              None, Nil, None, Nil,
              None, None,
              None,
              _
            ),
            label,
            resourceName,
            alias
          ) if selectList.valuesIterator.map(_.expr).forall(isPhysicalColumnRefTo(tbl.label, tbl.tableName, _)) =>
            // yep, we did - instead of a FromStatement, we can just
            // return a FromTable with enough info to rewrite any
            // VirtualColumn references to the ex-Statement into
            // PhysicalColumn references to the table.
            val replacementFrom = FromTable(tbl.tableName, tbl.definiteResourceName, alias, label, tbl.columns, tbl.primaryKeys)
            val newMap = selectList.foldLeft(currentMap) { case (acc, (columnLabel, namedExpr)) =>
              val PhysicalColumn(_, _, physCol, _) = namedExpr.expr
              acc + ((label, columnLabel) -> (physCol, tbl.tableName))
            }
            (newMap, replacementFrom)
          case other =>
            (currentMap, other)
        }
    }
  }

  private def isPhysicalColumnRefTo(table: AutoTableLabel, name: DatabaseTableName, e: Expr): Boolean = {
    e match {
      case PhysicalColumn(t, n, _, _) => t == table && n == name
      case _ => false
    }
  }

  private class Relabeller(relabelMap: RelabelMap) {
    def relabel(stmt: Statement): Statement =
      stmt match {
        case ct@CombinedTables(op, left, right) =>
          ct.copy(left = relabel(left), right = relabel(right))
        case cte@CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
          cte.copy(
            definitionQuery = relabel(defQuery),
            useQuery = relabel(useQuery)
          )
        case v@Values(labels, values) =>
          v.copy(values = values.map(_.map(relabel(_))))
        case sel@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
          sel.copy(
            relabel(distinctiveness),
            selectList.withValuesMapped { namedExpr => namedExpr.copy(expr = relabel(namedExpr.expr)) },
            relabel(from),
            where.map(relabel(_)),
            groupBy.map(relabel(_)),
            having.map(relabel(_)),
            orderBy.map(relabel(_))
          )
      }

    def relabel(expr: Expr): Expr =
      expr match {
        case vc@VirtualColumn(tbl, col, typ) =>
          relabelMap.get((tbl, col)) match {
            case Some((physCol, tableName)) => PhysicalColumn(tbl, tableName, physCol, typ)(vc.position)
            case None => vc
          }
        case otherAtomic: AtomicExpr =>
          otherAtomic
        case fc@FunctionCall(func, args) =>
          FunctionCall(func, args.map(relabel(_)))(fc.position)
        case afc@AggregateFunctionCall(func, args, distinct, filter) =>
          AggregateFunctionCall(func, args.map(relabel(_)), distinct, filter.map(relabel(_)))(afc.position)
        case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
          WindowedFunctionCall(
            func,
            args.map(relabel(_)),
            filter.map(relabel(_)),
            partitionBy.map(relabel(_)),
            orderBy.map(relabel(_)),
            frame
          )(wfc.position)
      }

    def relabel(ob: OrderBy): OrderBy =
      ob.copy(expr = relabel(ob.expr))

    def relabel(d: Distinctiveness): Distinctiveness =
      d match {
        case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() => d
        case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(relabel(_)))
      }

    def relabel(f: From): From =
      f.map[MT](
        relabelAtomicFrom(_),
        { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, relabelAtomicFrom(right), relabel(on)) }
      )

    def relabelAtomicFrom(f: AtomicFrom): AtomicFrom =
      f match {
        case fsr: FromSingleRow => fsr
        case ft: FromTable => ft
        case fs: FromStatement => fs.copy(statement = relabel(fs.statement))
      }
  }
}

/** Attempt to preserve ordering from inner queries to outer ones.
  * SelectListReferences must not be present (this is unchecked!!). */
object RemoveUnusedOrderBy {
  def apply[MT <: MetaTypes](stmt: Statement[MT], preserveTopLevelOrdering: Boolean = true): Statement[MT] = {
    new RemoveUnusedOrderBy[MT]().rewriteStatement(stmt, preserveTopLevelOrdering)
  }
}
