package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

class Merger[RNS, CT, CV](and: MonomorphicFunction[CT]) {
  def merge(stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] =
    stmt match {
      case c@CombinedTables(_, left, right) =>
        c.copy(left = merge(left), right = merge(right))
      case cte@CTE(_defLabel, defQ, _label, useQ) =>
        // TODO: maybe make this not a CTE at all, sometimes?
        cte.copy(definitionQuery = merge(defQ), useQuery = merge(useQ))
      case v: Values[CT, CV] =>
        v
      case s: Select[RNS, CT, CV] =>
        mergeSelect(s)
    }

  private def mergeSelect(select: Select[RNS, CT, CV]): Statement[RNS, CT, CV] =
    select.from match {
      case FromStatement(stmt2, label, alias) =>
        merge(stmt2) match {
          case subselect: Select[RNS, CT, CV] =>
            actuallyMerge(select, subselect, label, alias) match {
              case Some(merged) =>
                merged
              case None =>
                select.copy(from = FromStatement(subselect, label, alias))
            }
          case other => select.copy(from = FromStatement(other, label, alias))
        }
      case _ =>
        select
    }

  private def actuallyMerge(superquery: Select[RNS, CT, CV], subquery: Select[RNS, CT, CV], fromLabel: TableLabel, fromAlias: Option[(RNS, ResourceName)]): Option[Statement[RNS, CT, CV]] =
    (subquery, superquery) match {
      case (_, b) if b.hint(SelectHint.NoChainMerge) =>
        None
      case (a, _) if a.isWindowed =>
        None
      case (a, b) if a.isAggregated && b.isAggregated =>
        None
      case _ =>
        None
    }

  private def mergeSelection(
    aTable: TableLabel,
    aColumns: OrderedMap[ColumnLabel, Expr[CT, CV]],
    b: OrderedMap[ColumnLabel, NamedExpr[CT, CV]]
  ): OrderedMap[ColumnLabel, NamedExpr[CT, CV]] = {
    b.withValuesMapped { bExpr =>
      bExpr.copy(expr = replaceRefs(aTable, aColumns, bExpr.expr))
    }
  }

  private def mergeOrderBy(
    aTable: TableLabel,
    aColumns: OrderedMap[ColumnLabel, Expr[CT, CV]],
    obA: Seq[OrderBy[CT, CV]],
    obB: Seq[OrderBy[CT, CV]]
  ): Seq[OrderBy[CT, CV]] = {
    obB.map { ob => ob.copy(expr = replaceRefs(aTable, aColumns, ob.expr)) } ++ obA
  }

  private def mergeWhereLike(
    aTable: TableLabel,
    aColumns: OrderedMap[ColumnLabel, Expr[CT, CV]],
    a: Option[Expr[CT, CV]],
    b: Option[Expr[CT, CV]]
  ): Option[Expr[CT, CV]] =
    (a, b) match {
      case (None, None) => None
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(replaceRefs(aTable, aColumns, b))
      case (Some(a), Some(b)) => Some(FunctionCall(and, Seq(a, replaceRefs(aTable, aColumns, b)))(NoPosition, NoPosition))
    }

  private def replaceRefs(aTable: TableLabel, aColumns: OrderedMap[ColumnLabel, Expr[CT, CV]], b: Expr[CT, CV]) =
    new ReplaceRefs(aTable, aColumns).go(b)

  private class ReplaceRefs(aTable: TableLabel, aColumns: OrderedMap[ColumnLabel, Expr[CT, CV]]) {
    def go(b: Expr[CT, CV]): Expr[CT, CV] =
      b match {
        case Column(`aTable`, c, t) =>
          aColumns.get(c) match {
            case Some(aExpr) if aExpr.typ == t =>
              aExpr
            case Some(_) =>
              oops("Found a maltyped column reference!")
            case None =>
              oops("Found a dangling column reference!")
          }
        case c: Column[CT] =>
          c
        case l: Literal[CT, CV] =>
          l
        case fc@FunctionCall(f, params) =>
          FunctionCall(f, params.map(go _))(fc.position, fc.functionNamePosition)
        case fc@AggregateFunctionCall(f, params, distinct, filter) =>
          AggregateFunctionCall(
            f,
            params.map(go _),
            distinct,
            filter.map(go _)
          )(fc.position, fc.functionNamePosition)
        case fc@WindowedFunctionCall(f, params, filter, partitionBy, orderBy, frame) =>
          WindowedFunctionCall(
            f,
            params.map(go _),
            filter.map(go _),
            partitionBy.map(go _),
            orderBy.map { ob => ob.copy(expr = go(ob.expr)) },
            frame
          )(fc.position, fc.functionNamePosition)
        case sr: SelectListReference[CT] =>
          // This is safe because we're creating a new query with the
          // same output as b's query, so this just refers to the new
          // (possibly rewritten) column in the same position
          sr
      }
  }

  private def oops(msg: String): Nothing = throw new Exception(msg)
}

object Merger {
  def combineLimits(aLim: Option[BigInt], aOff: Option[BigInt], bLim: Option[BigInt], bOff: Option[BigInt]): (Option[BigInt], Option[BigInt]) = {
    // ok, what we're doing here is basically finding the intersection of two segments of
    // the integers, where either segment may end at infinity.  Note that all the inputs are
    // non-negative (enforced by the parser).  This simplifies things somewhat
    // because we can assume that aOff + bOff > aOff
    require(aLim.fold(true)(_ >= 0), "Negative aLim")
    require(aOff.fold(true)(_ >= 0), "Negative aOff")
    require(bLim.fold(true)(_ >= 0), "Negative bLim")
    require(bOff.fold(true)(_ >= 0), "Negative bOff")
    (aLim, aOff, bLim, bOff) match {
      case (None, None, bLim, bOff) =>
        (bLim, bOff)
      case (None, Some(aOff), bLim, bOff) =>
        // first is unbounded to the right
        (bLim, Some(aOff + bOff.getOrElse(BigInt(0))))
      case (Some(aLim), aOff, Some(bLim), bOff) =>
        // both are bound to the right
        val trueAOff = aOff.getOrElse(BigInt(0))
        val trueBOff = bOff.getOrElse(BigInt(0)) + trueAOff
        val trueAEnd = trueAOff + aLim
        val trueBEnd = trueBOff + bLim

        val trueOff = trueBOff min trueAEnd
        val trueEnd = trueBEnd min trueAEnd
        val trueLim = trueEnd - trueOff

        (Some(trueLim), Some(trueOff))
      case (Some(aLim), aOff, None, bOff) =>
        // first is bound to the right but the second is not
        val trueAOff = aOff.getOrElse(BigInt(0))
        val trueBOff = bOff.getOrElse(BigInt(0)) + trueAOff

        val trueEnd = trueAOff + aLim
        val trueOff = trueBOff min trueEnd
        val trueLim = trueEnd - trueOff
        (Some(trueLim), Some(trueOff))
    }
  }
}
