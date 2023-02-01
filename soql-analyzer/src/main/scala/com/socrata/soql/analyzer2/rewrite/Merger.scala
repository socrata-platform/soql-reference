package com.socrata.soql.analyzer2.rewrite

import scala.annotation.tailrec
import scala.util.parsing.input.NoPosition

import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc
import com.socrata.soql.analyzer2._

class Merger[MT <: MetaTypes](and: MonomorphicFunction[MT#CT]) extends SoQLAnalyzerUniverse[MT] with SoQLAnalyzerExpressions[MT] with LabelHelper[MT] {
  private implicit val hd = new HasDoc[CV] {
    override def docOf(v: CV) = com.socrata.prettyprint.Doc(v.toString)
  }

  def merge(stmt: Statement): Statement = {
    val r = doMerge(stmt)
    debug("finished", r)
    debugDone()
    r
  }

  private def doMerge(stmt: Statement): Statement =
    stmt match {
      case c@CombinedTables(_, left, right) =>
        debug("combined tables")
        c.copy(left = doMerge(left), right = doMerge(right))
      case cte@CTE(_defLabel, _defAlias, defQ, _label, useQ) =>
        // TODO: maybe make this not a CTE at all, sometimes?
        debug("CTE")
        cte.copy(definitionQuery = doMerge(defQ), useQuery = doMerge(useQ))
      case v: Values =>
        debug("values")
        v
      case select: Select =>
        debug("select")
        select.copy(from = mergeFrom(select.from)) match {
          case b@Select(_, _, Unjoin(FromStatement(a: Select, aLabel, aResourceName, aAlias), bRejoin), _, _, _, _, _, _, _, _) =>
            // This privileges the first query in b's FROM because our
            // queries are frequently constructed in a chain.
            mergeSelects(a, aLabel, aResourceName, aAlias, b, bRejoin) match {
              case None =>
                debug("declained to merge")
                b
              case Some(merged) =>
                debug("merged")
                merged
            }
          case other: Select =>
            debug("select on not-from-select")
            other
          case other =>
            debug("Not a select")
            other
        }
    }

  private def mergeFrom(from: From): From = {
    from.map[MT](
      mergeAtomicFrom,
      (jt, lat, left, right, on) => Join(jt, lat, left, mergeAtomicFrom(right), on)
    )
  }

  private def mergeAtomicFrom(from: AtomicFrom): AtomicFrom = {
    from match {
      case s@FromStatement(stmt, _, _, _) => s.copy(statement = doMerge(stmt))
      case other => other
    }
  }

  private type ExprRewriter = Expr => Expr
  private type FromRewriter = (From, ExprRewriter) => From

  private object Unjoin {
    // case ... join@Unjoin(leftmost, rebuild) ... =>
    //    `leftmost` is now the leftmost atomic From in `join`
    //    and rebuild is a function that takes a new from and
    //    left-appends it to the remainder of `join`.  Upon
    //    append, a rewrite function is applied to the items in
    //    the original join (which we use to rewrite "on" and
    //    lateral subqueries when merging.
    //
    //    One thing to note: rebuild(leftmost, identity) == join
    def unapply(x: From): Some[(AtomicFrom, FromRewriter)] = {
      @tailrec
      def loop(here: From, stack: List[FromRewriter]): (AtomicFrom, List[FromRewriter]) = {
        here match {
          case atom: AtomicFrom => (atom, stack)
          case Join(jt, lat, left, right, on) => loop(left, { (newLeft: From, xform: ExprRewriter) => Join(jt, lat, newLeft, if(lat) rewrite(right, xform) else right, xform(on)) } :: stack)
        }
      }

      val (leftmost, stack) = loop(x, Nil)
      Some((leftmost, { (from, rewriteExpr) => stack.foldLeft(from) { (acc, build) => build(acc, rewriteExpr) } }))
    }
  }

  private def rewrite(from: AtomicFrom, xform: ExprRewriter): AtomicFrom =
    from match {
      case fs@FromStatement(s, _, _, _) => fs.copy(statement = rewrite(s, xform))
      case other => other
    }

  private def rewrite(s: Statement, xform: ExprRewriter): Statement =
    s match {
      case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        Select(
          rewrite(distinctiveness, xform),
          rewrite(selectList, xform),
          from.map(
            rewrite(_, xform),
            (jt, lat, left, right, on) => Join(jt, lat, left, rewrite(right, xform), xform(on))
          ),
          where.map(xform), groupBy.map(xform), having.map(xform), orderBy.map { ob => ob.copy(expr=xform(ob.expr)) },
          limit, offset, search, hint)
      case Values(labels, vs) => Values(labels, vs.map(_.map(xform)))
      case CombinedTables(op, left, right) => CombinedTables(op, rewrite(left, xform), rewrite(right, xform))
      case CTE(defLbl, defAlias, defQ, useLbl, useQ) => CTE(defLbl, defAlias, rewrite(defQ, xform), useLbl, rewrite(useQ, xform))
    }
  private def rewrite(d: Distinctiveness, xform: ExprRewriter): Distinctiveness =
    d match {
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(xform))
      case x@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => x
    }
  private def rewrite(d: OrderedMap[AutoColumnLabel, NamedExpr], xform: ExprRewriter): OrderedMap[AutoColumnLabel, NamedExpr] =
    d.withValuesMapped { ne => ne.copy(expr = xform(ne.expr)) }

  private def mergeSelects(
    a: Select, aLabel: AutoTableLabel, aResourceName: Option[ScopedResourceName[RNS]], aAlias: Option[ResourceName],
    b: Select, bRejoin: FromRewriter
  ): Option[Statement] =
    // If we decide to merge this, we're going to create some flavor of
    //   select merged_projection from bRejoin(FromStatement(a.from)) ...)
    (a, b) match {
      case (a, b) if definitelyRequiresSubselect(a, aLabel, b) =>
        debug("declaining to merge - definitely requires subselect")
        None
      case (a, Select(bDistinct, bSelect, _oldA, None, Nil, None, Nil, bLim, bOff, None, bHint)) =>
        // Just projection change + possibly limit/offset and
        // distinctiveness.  We can merge this onto almost anything,
        // and what we can't merge it onto has been rejected by
        // definitelyRequiresSubselect - with one exception!  It's
        // possible for a parent's group-by to have been implicit in
        // its select list, so we'll only accept this merge if the
        // resulting query's aggregatedness is the same as the
        // parent's.
        debug("simple", a, b)
        val (newLim, newOff) = Merger.combineLimits(a.limit, a.offset, bLim, bOff)
        val selectList = mergeSelection(aLabel, a.selectedExprs, bSelect)
        Some(
          a.copy(
            distinctiveness = mergeDistinct(aLabel, a.selectedExprs, bDistinct),
            selectList = selectList,
            from = bRejoin(a.from, replaceRefs(aLabel, a.selectedExprs, _)),
            orderBy = orderByVsDistinct(a.orderBy, selectList.withValuesMapped(_.expr), bDistinct),
            limit = newLim,
            offset = newOff,
            hint = a.hint ++ bHint
          )
        ).filter(_.isAggregated == a.isAggregated)

      case (a@Select(_aDistinct, aSelect, aFrom, aWhere, Nil, None, aOrder, None, None, None, aHint),
            b@Select(bDistinct, bSelect, _oldA, bWhere, Nil, None, bOrder, bLim, bOff, None, bHint)) if !b.isAggregated =>
        debug("non-aggregate on non-aggregate")
        val selectList = mergeSelection(aLabel, a.selectedExprs, bSelect)
        Some(a.copy(
               distinctiveness = mergeDistinct(aLabel, a.selectedExprs, bDistinct),
               selectList = selectList,
               from = bRejoin(a.from, replaceRefs(aLabel, a.selectedExprs, _)),
               where = mergeWhereLike(aLabel, a.selectedExprs, aWhere, bWhere),
               orderBy = mergeOrderBy(aLabel, a.selectedExprs, orderByVsDistinct(aOrder, selectList.withValuesMapped(_.expr), bDistinct), bOrder),
               limit = bLim,
               offset = bOff,
               hint = a.hint ++ bHint
             ))

      case (a@Select(_aDistinct, aSelect, aFrom, aWhere, Nil, None, _aOrder, None, None, None, aHint),
            b@Select(bDistinct, bSelect, _oldA, bWhere, bGroup, bHaving, bOrder, bLim, bOff, None, bHint)) if b.isAggregated =>
        debug("aggregate on non-aggregate")
        Some(Select(
               distinctiveness = mergeDistinct(aLabel, a.selectedExprs, bDistinct),
               selectList = mergeSelection(aLabel, a.selectedExprs, bSelect),
               from = bRejoin(aFrom, replaceRefs(aLabel, a.selectedExprs, _)),
               where = mergeWhereLike(aLabel, a.selectedExprs, aWhere, bWhere),
               groupBy = mergeGroupBy(aLabel, a.selectedExprs, bGroup),
               having = mergeWhereLike(aLabel, a.selectedExprs, None, bHaving),
               orderBy = mergeOrderBy(aLabel, a.selectedExprs, Nil, bOrder),
               limit = bLim,
               offset = bOff,
               search = None,
               hint = aHint ++ bHint))

      case (a@Select(_aDistinct, aSelect, aFrom, aWhere, aGroup, aHaving, aOrder, None, None, None, aHint),
            b@Select(bDistinct, bSelect, _oldA, bWhere, Nil, None, bOrder, bLim, bOff, None, bHint)) if a.isAggregated =>
        debug("non-aggregate on aggregate")
        Some(
          Select(
            distinctiveness = mergeDistinct(aLabel, a.selectedExprs, bDistinct),
            selectList = mergeSelection(aLabel, a.selectedExprs, bSelect),
            from = bRejoin(aFrom, replaceRefs(aLabel, a.selectedExprs, _)),
            where = aWhere,
            groupBy = aGroup,
            having = mergeWhereLike(aLabel, a.selectedExprs, aHaving, bWhere),
            orderBy = mergeOrderBy(aLabel, a.selectedExprs, aOrder, bOrder),
            limit = bLim,
            offset = bOff,
            search = None,
            hint = aHint ++ bHint)
        ).filter(_.isAggregated) // again, just in case the aggregation was implicit and our merge removed it
      case _ =>
        debug("decline to merge - unknown pattern")
        None
    }

  private def definitelyRequiresSubselect(a: Select, aLabel: AutoTableLabel, b: Select): Boolean = {
    if(b.hint(SelectHint.NoChainMerge)) {
      debug("B asks not to merge with its upstream")
      return true
    }

    if(b.search.isDefined) {
      debug("B has a search")
      return true
    }

    if(a.distinctiveness != Distinctiveness.Indistinct()) {
      // selecting from a DISTINCT query is tricky to merge; let's
      // not.
      debug("A is distinct in some way")
      return true
    }

    if(b.isWindowed || b.isAggregated || b.distinctiveness != Distinctiveness.Indistinct()) {
      if(a.limit.isDefined || a.offset.isDefined) {
        // can't smash the queries together because a trims out some
        // of its rows after-the-fact, so b's windows (or groups)
        // shouldn't see those trimmed-out rows.
        debug("B cares about windows or groups, and A trims leading/trailing rows")
        return true
      }
    }

    if(a.isAggregated) {
      if(b.isAggregated) {
        // can't aggregate-on-aggregate in a single query
        debug("aggregate-on-aggregate")
        return true
      }

      if(!b.from.isInstanceOf[AtomicFrom]) {
        // b multiplies the rows, which affects grouping; can't merge.
        debug("join-on-aggregate")
        return true
      }
    }

    // We _can_ merge windowed-on-the-left _sometimes_
    if(a.isWindowed) {
      // We don't care about window function calls that wouldn't show
      // up in the merged query.
      val windowUsed =
        a.orderBy.exists(_.expr.isWindowed) ||
          a.selectList.iterator.filter(_._2.expr.isWindowed).exists { case (k, namedExpr) =>
            b.directlyContains(VirtualColumn(aLabel, k, namedExpr.expr.typ)(AtomicPositionInfo.None))
          }

      if(windowUsed) {
        // Ok, so there is a window function column in a which is used
        // in b Will that column's computation still be valid if b is
        // squashed onto a?
        if(shapeChanging(b)) {
          // Changing the shape of the result-set => would affect the
          // windows seen => need a subselect
          debug("shape-changing on windowed")
          return true
        }

        // Ok, so the output of this b is a 1:1 map of the output of a
        // (+/- limit/offset) so we can merge window functions if they
        // are not themselves used within window functions.
        if(b.isWindowed) {
          val windowsWithinWindows =
            a.selectList.iterator.filter(_._2.expr.isWindowed).exists { case (k, namedExpr) =>
              val target = VirtualColumn(aLabel, k, namedExpr.expr.typ)(AtomicPositionInfo.None)
              b.directlyFind {
                case e: WindowedFunctionCall => e.contains(target)
                case _ => false
              }.isDefined
            }

          if(windowsWithinWindows) {
            debug("windows within windows")
            return true
          }
        }
      }
    }

    debug("no a-priori reason not to merge")
    false
  }

  // true if "q" reorders, groups, or filters its input-rows
  private def shapeChanging(q: Select): Boolean =
    q.isAggregated ||
      !q.from.isInstanceOf[AtomicFrom] ||
      q.where.isDefined ||
      q.orderBy.nonEmpty ||
      q.distinctiveness != Distinctiveness.Indistinct()

  private def mergeDistinct(
    aTable: AutoTableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr],
    b: Distinctiveness
  ): Distinctiveness =
    b match {
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(replaceRefs(aTable, aColumns, _)))
      case x@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => x
    }

  private def mergeSelection(
    aTable: AutoTableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr],
    b: OrderedMap[AutoColumnLabel, NamedExpr]
  ): OrderedMap[AutoColumnLabel, NamedExpr] =
    b.withValuesMapped { bExpr =>
      bExpr.copy(expr = replaceRefs(aTable, aColumns, bExpr.expr))
    }

  private def mergeGroupBy(aTable: AutoTableLabel, aColumns: OrderedMap[AutoColumnLabel, Expr], gb: Seq[Expr]): Seq[Expr] = {
    gb.map(replaceRefs(aTable, aColumns, _))
  }

  private def orderByVsDistinct(orderBy: Seq[OrderBy], columns: OrderedMap[AutoColumnLabel, Expr], bDistinct: Distinctiveness) = {
    orderBy.filter { ob =>
      bDistinct match {
        case Distinctiveness.Indistinct() => true
        case Distinctiveness.FullyDistinct() => columns.values.exists(_ == ob.expr)
        case Distinctiveness.On(exprs) => exprs.contains(ob.expr)
      }
    }
  }

  private def mergeOrderBy(
    aTable: AutoTableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr],
    obA: Seq[OrderBy],
    obB: Seq[OrderBy]
  ): Seq[OrderBy] =
    obB.map { ob => ob.copy(expr = replaceRefs(aTable, aColumns, ob.expr)) } ++ obA

  private def mergeWhereLike(
    aTable: AutoTableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr],
    a: Option[Expr],
    b: Option[Expr]
  ): Option[Expr] =
    (a, b) match {
      case (None, None) => None
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(replaceRefs(aTable, aColumns, b))
      case (Some(a), Some(b)) => Some(FunctionCall(and, Seq(a, replaceRefs(aTable, aColumns, b)))(FuncallPositionInfo.None))
    }

  private def replaceRefs(aTable: AutoTableLabel, aColumns: OrderedMap[AutoColumnLabel, Expr], b: Expr) =
    new ReplaceRefs(aTable, aColumns).go(b)

  private class ReplaceRefs(aTable: AutoTableLabel, aColumns: OrderedMap[AutoColumnLabel, Expr]) {
    def go(b: Expr): Expr =
      b match {
        case VirtualColumn(`aTable`, c : AutoColumnLabel, t) =>
          aColumns.get(c) match {
            case Some(aExpr) if aExpr.typ == t =>
              aExpr
            case Some(_) =>
              oops("Found a maltyped column reference!")
            case None =>
              oops("Found a dangling column reference!")
          }
        case c: Column =>
          c
        case l: Literal =>
          l
        case fc@FunctionCall(f, params) =>
          FunctionCall(f, params.map(go _))(fc.position)
        case fc@AggregateFunctionCall(f, params, distinct, filter) =>
          AggregateFunctionCall(
            f,
            params.map(go _),
            distinct,
            filter.map(go _)
          )(fc.position)
        case fc@WindowedFunctionCall(f, params, filter, partitionBy, orderBy, frame) =>
          WindowedFunctionCall(
            f,
            params.map(go _),
            filter.map(go _),
            partitionBy.map(go _),
            orderBy.map { ob => ob.copy(expr = go(ob.expr)) },
            frame
          )(fc.position)
        case sr: SelectListReference =>
          // This is safe because we're creating a new query with the
          // same output as b's query, so this just refers to the new
          // (possibly rewritten) column in the same position
          sr
      }
  }

  private def oops(msg: String): Nothing = throw new Exception(msg)

  private var debugOne = false
  private def debugLine(s: => Any): Unit = { }
  private def debug(message: String): Unit = {
    if(debugOne) {
      debugLine("-------------------------------------------------------------------------------")
    } else {
      debugLine("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv MERGE vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
      debugOne = true
    }
    debugLine(message)
  }
  private def debugDone(): Unit = {
    if(debugOne) {
      debugLine("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
      debugOne = false
    }
  }
  private def debug(message: String, a: Statement, bs: Statement*): Unit = {
    debug(message)
    debug(a.debugStr)
    bs.foreach { b => debug(b.debugStr) }
  }
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
