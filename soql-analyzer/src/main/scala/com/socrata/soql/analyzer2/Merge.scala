package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.NoPosition

import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

class Merger[RNS, CT, CV](and: MonomorphicFunction[CT]) {
  def merge(stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] = {
    val r = doMerge(stmt)
    debug("finished", r)
    debugDone()
    r
  }

  private def doMerge(stmt: Statement[RNS, CT, CV]): Statement[RNS, CT, CV] =
    stmt match {
      case c@CombinedTables(_, left, right) =>
        c.copy(left = doMerge(left), right = doMerge(right))
      case cte@CTE(_defLabel, defQ, _label, useQ) =>
        // TODO: maybe make this not a CTE at all, sometimes?
        cte.copy(definitionQuery = doMerge(defQ), useQuery = doMerge(useQ))
      case v: Values[CT, CV] =>
        v
      case select: Select[RNS, CT, CV] =>
        select.copy(from = mergeFrom(select.from)) match {
          case b@Select(_, _, Unjoin(FromStatement(a: Select[RNS, CT, CV], aLabel, aAlias), bRejoin), _, _, _, _, _, _, _, _) =>
            // This privileges the first query in b's FROM because our
            // queries are frequently constructed in a chain.
            mergeSelects(a, aLabel, aAlias, b, bRejoin) match {
              case None =>
                b
              case Some(merged) =>
                merged
            }
          case other =>
            other
        }
    }

  private def mergeFrom(from: From[RNS, CT, CV]): From[RNS, CT, CV] = {
    from.map[RNS, CT, CV](
      mergeAtomicFrom,
      (jt, lat, left, right, on) => Join(jt, lat, left, mergeAtomicFrom(right), on)
    )
  }

  private def mergeAtomicFrom(from: AtomicFrom[RNS, CT, CV]): AtomicFrom[RNS, CT, CV] = {
    from match {
      case FromStatement(stmt, label, alias) => FromStatement(doMerge(stmt), label, alias)
      case other => other
    }
  }

  private type ExprRewriter = Expr[CT, CV] => Expr[CT, CV]
  private type FromRewriter = (From[RNS, CT, CV], ExprRewriter) => From[RNS, CT, CV]

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
    def unapply(x: From[RNS, CT, CV]): Some[(AtomicFrom[RNS, CT, CV], FromRewriter)] = {
      @tailrec
      def loop(here: From[RNS, CT, CV], stack: List[FromRewriter]): (AtomicFrom[RNS, CT, CV], List[FromRewriter]) = {
        here match {
          case atom: AtomicFrom[RNS, CT, CV] => (atom, stack)
          case Join(jt, lat, left, right, on) => loop(left, { (newLeft: From[RNS, CT, CV], xform: ExprRewriter) => Join(jt, lat, newLeft, if(lat) rewrite(right, xform) else right, xform(on)) } :: stack)
        }
      }

      val (leftmost, stack) = loop(x, Nil)
      Some((leftmost, { (from, rewriteExpr) => stack.foldLeft(from) { (acc, build) => build(acc, rewriteExpr) } }))
    }
  }

  private def rewrite(from: AtomicFrom[RNS, CT, CV], xform: ExprRewriter): AtomicFrom[RNS, CT, CV] =
    from match {
      case fs@FromStatement(s, _, _) => fs.copy(statement = rewrite(s, xform))
      case other => other
    }

  private def rewrite(s: Statement[RNS, CT, CV], xform: ExprRewriter): Statement[RNS, CT, CV] =
    s match {
      case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        Select(
          rewrite(distinctiveness, xform),
          rewrite(selectList, xform),
          from.map[RNS, CT, CV](
            rewrite(_, xform),
            (jt, lat, left, right, on) => Join(jt, lat, left, rewrite(right, xform), xform(on))
          ),
          where.map(xform), groupBy.map(xform), having.map(xform), orderBy.map { ob => ob.copy(expr=xform(ob.expr)) },
          limit, offset, search, hint)
      case Values(vs) => Values(vs.map(_.map(xform)))
      case CombinedTables(op, left, right) => CombinedTables(op, rewrite(left, xform), rewrite(right, xform))
      case CTE(defLbl, defQ, useLbl, useQ) => CTE(defLbl, rewrite(defQ, xform), useLbl, rewrite(useQ, xform))
    }
  private def rewrite(d: Distinctiveness[CT, CV], xform: ExprRewriter): Distinctiveness[CT, CV] =
    d match {
      case Distinctiveness.Indistinct => Distinctiveness.Indistinct
      case Distinctiveness.FullyDistinct => Distinctiveness.FullyDistinct
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(xform))
    }
  private def rewrite(d: OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]], xform: ExprRewriter): OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]] =
    d.withValuesMapped { ne => ne.copy(expr = xform(ne.expr)) }

  private def mergeSelects(
    a: Select[RNS, CT, CV], aLabel: TableLabel, aAlias: Option[(RNS, ResourceName)],
    b: Select[RNS, CT, CV], bRejoin: FromRewriter
  ): Option[Statement[RNS, CT, CV]] =
    // If we decide to merge this, we're going to create some flavor of
    //   select merged_projection from bRejoin(FromStatement(a.from)) ...)
    (a, b) match {
      case (a, b) if definitelyRequiresSubselect(a, aLabel, b) =>
        None
      case (a, Select(bDistinct, bSelect, _oldA, None, Nil, None, Nil, bLim, bOff, None, bHint)) =>
        // Just projection change + possibly limit/offset and
        // distinctiveness.  We can merge this onto almost anything,
        // and what we can't merge it onto has been rejected by
        // definitelyRequiresSubselect.
        debug("simple", a, b)
        val (newLim, newOff) = Merger.combineLimits(a.limit, a.offset, bLim, bOff)
        Some(a.copy(
               selectList = mergeSelection(aLabel, a.selectedExprs, bSelect),
               from = bRejoin(a.from, replaceRefs(aLabel, a.selectedExprs, _)),
               limit = newLim,
               offset = newOff,
               hint = a.hint ++ bHint
             ))

      case (a@Select(_aDistinct, aSelect, aFrom, aWhere, Nil, None, aOrder, None, None, None, aHint),
            b@Select(bDistinct, bSelect, _oldA, bWhere, Nil, None, bOrder, bLim, bOff, None, bHint)) if !b.isAggregated =>
        debug("non-aggregate on non-aggregate", a, b)
        Some(a.copy(
               selectList = mergeSelection(aLabel, a.selectedExprs, bSelect),
               from = bRejoin(a.from, replaceRefs(aLabel, a.selectedExprs, _)),
               where = mergeWhereLike(aLabel, a.selectedExprs, aWhere, bWhere),
               orderBy = mergeOrderBy(aLabel, a.selectedExprs, aOrder, bOrder),
               limit = bLim,
               offset = bOff,
               hint = a.hint ++ bHint
             ))

      case (a@Select(_aDistinct, aSelect, aFrom, aWhere, Nil, None, _aOrder, None, None, None, aHint),
            b@Select(bDistinct, bSelect, _oldA, bWhere, bGroup, bHaving, bOrder, bLim, bOff, None, bHint)) if b.isAggregated =>
        debug("aggregate on non-aggregate", a, b)
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
            b@Select(bDistinct, bSelect, _oldA, bWhere, bGroup, bHaving, bOrder, bLim, bOff, None, bHint)) if a.isAggregated =>
        debug("non-aggregate on aggregate", a, b)
        Some(Select(
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
               hint = aHint ++ bHint))
      case _ =>
        None
    }

  private def definitelyRequiresSubselect(a: Select[RNS, CT, CV], aLabel: TableLabel, b: Select[RNS, CT, CV]): Boolean = {
    if(b.hint(SelectHint.NoChainMerge)) {
      debug("B asks not to merge with its upstream", a, b)
      return true
    }

    if(b.search.isDefined) {
      debug("B has a search", a, b)
      return true
    }

    if(a.distinctiveness != Distinctiveness.Indistinct) {
      // selecting from a DISTINCT query is tricky to merge; let's
      // not.
      debug("A is distinct in some way", a, b)
      return true
    }

    if(b.isWindowed || b.isAggregated || b.distinctiveness != Distinctiveness.Indistinct) {
      if(a.limit.isDefined || a.offset.isDefined) {
        // can't smash the queries together because a trims out some
        // of its rows after-the-fact, so b's windows (or groups)
        // shouldn't see those trimmed-out rows.
        debug("B cares about windows or groups, and A trims leading/trailing rows", a, b)
        return true
      }
    }

    if(a.isAggregated) {
      if(b.isAggregated) {
        // can't aggregate-on-aggregate in a single query
        debug("aggregate-on-aggregate", a, b)
        return true
      }

      if(!b.from.isInstanceOf[AtomicFrom[_, _, _]]) {
        // b multiplies the rows, which affects grouping; can't merge.
        debug("join-on-aggregate", a, b)
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
            b.directlyContains(Column(aLabel, k, namedExpr.expr.typ)(NoPosition))
          }

      if(windowUsed) {
        // Ok, so there is a window function column in a which is used
        // in b Will that column's computation still be valid if b is
        // squashed onto a?
        if(shapeChanging(b)) {
          // Changing the shape of the result-set => would affect the
          // windows seen => need a subselect
          debug("shape-changing on windowed", a, b)
          return true
        }

        // Ok, so the output of this b is a 1:1 map of the output of a
        // (+/- limit/offset) so we can merge window functions if they
        // are not themselves used within window functions.
        if(b.isWindowed) {
          val windowsWithinWindows =
            a.selectList.iterator.filter(_._2.expr.isWindowed).exists { case (k, namedExpr) =>
              val target = Column(aLabel, k, namedExpr.expr.typ)(NoPosition)
              b.directlyFind {
                case e: WindowedFunctionCall[CT, CV] => e.contains(target)
                case _ => false
              }.isDefined
            }

          if(windowsWithinWindows) {
            debug("windows within windows", a, b)
            return true
          }
        }
      }
    }

    debug("no a-priori reason not to merge", a, b)
    false
  }

  // true if "q" reorders, groups, or filters its input-rows
  private def shapeChanging(q: Select[RNS, CT, CV]): Boolean =
    q.isAggregated ||
      !q.from.isInstanceOf[AtomicFrom[_, _, _]] ||
      q.where.isDefined ||
      q.orderBy.nonEmpty ||
      q.distinctiveness != Distinctiveness.Indistinct

  private var debugOne = false
  private def debugLine(s: => Any): Unit = {}
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
  private def debug(message: String, a: Statement[RNS, CT, CV], bs: Statement[RNS, CT, CV]*): Unit = {
    implicit val hd = new HasDoc[CV] {
      override def docOf(v: CV) = com.socrata.prettyprint.Doc(v.toString)
    }
    debug(message)
    debug(a.debugStr)
    bs.foreach { b => debug(b.debugStr) }
  }

  private def mergeUnjoined(superquery: Select[RNS, CT, CV], subquery: Select[RNS, CT, CV], fromLabel: TableLabel, fromAlias: Option[(RNS, ResourceName)]): Option[Select[RNS, CT, CV]] =
    // Switch this around because I'm thinking about this as "select subquery |> select superquery"
    // rather than "select superquery from (subquery)"
    (subquery, superquery) match {
      case (_, b) if b.hint(SelectHint.NoChainMerge) =>
        None
      case (a, b) if a.isWindowed && (b.isWindowed || b.isAggregated) =>
        System.err.println("MERGE: ABANDON: PARENT WINDOWED AND CHILD WINDOWED OR AGGREGATED")
        None
      case (a, b) if a.isAggregated && b.isAggregated =>
        System.err.println("MERGE: ABANDON: PARENT AND CHILD BOTH AGGREGATED")
        None
      case (a, Select(Distinctiveness.Indistinct, bSelect, _oldA, None, Nil, None, Nil, None, None, None, bHint)) =>
        // Just projection change; we can merge this onto anything
        System.err.println("MERGE: SIMPLE PROJECTION CHANGE")
        Some(a.copy(
               selectList = mergeSelection(fromLabel, a.selectedExprs, bSelect),
               hint = a.hint ++ bHint
             ))
      case (a, b@Select(Distinctiveness.Indistinct, bSelect, _oldA, None, Nil, None, Nil, bLim, bOff, None, bHint)) if !b.isAggregated =>
        // Just projection + limit/offset change; we can merge this onto (almost) anything
        System.err.println("MERGE: PROJECTION + LIMOFF CHANGE")
        val (newLim, newOff) = Merger.combineLimits(a.limit, a.offset, bLim, bOff)
        Some(a.copy(
               selectList = mergeSelection(fromLabel, a.selectedExprs, bSelect),
               limit = newLim,
               offset = newOff,
               hint = a.hint ++ bHint
             ))

      case (a@Select(Distinctiveness.Indistinct, aSelect, aFrom, aWhere, Nil, None, aOrder, None, None, aSearch, aHint),
            b@Select(Distinctiveness.Indistinct, bSelect, _oldA, bWhere, Nil, None, bOrder, bLim, bOff, None, bHint))
          if !a.isWindowed && !a.isAggregated && !b.isAggregated =>
        // Non-aggregate on unwindowed non-aggregate change of filter + possible new limit/offset
        System.err.println("MERGE: SIMPLE PROJECTION CHANGE")
        Some(Select(distinctiveness = Distinctiveness.Indistinct,
                    selectList = mergeSelection(fromLabel, a.selectedExprs, bSelect),
                    from = aFrom,
                    where = mergeWhereLike(fromLabel, a.selectedExprs, aWhere, bWhere),
                    groupBy = Nil,
                    having = None,
                    orderBy = mergeOrderBy(fromLabel, a.selectedExprs, aOrder, bOrder),
                    limit = bLim,
                    offset = bOff,
                    search = aSearch,
                    hint = aHint ++ bHint))

    case (a@Select(Distinctiveness.Indistinct, aSelect, aFrom, aWhere, Nil, None, _aOrder, None, None, None, aHints),
          b@Select(Distinctiveness.Indistinct, bSelect, _oldA, bWhere, bGroup, bHaving, bOrder, bLim, bOff, None, bHints)) if b.isAggregated || b.isWindowed =>
        // an aggregate on a non-aggregate
        System.err.println("MERGE: AGGREGATE ON NON-AGGREGATE")
        Some(Select(distinctiveness = Distinctiveness.Indistinct,
                    selectList = mergeSelection(fromLabel, a.selectedExprs, bSelect),
                    from = aFrom,
                    where = mergeWhereLike(fromLabel, a.selectedExprs, aWhere, bWhere),
                    groupBy = mergeGroupBy(fromLabel, a.selectedExprs, bGroup),
                    having = mergeWhereLike(fromLabel, a.selectedExprs, None, bHaving),
                    orderBy = mergeOrderBy(fromLabel, a.selectedExprs, Nil, bOrder),
                    limit = bLim,
                    offset = bOff,
                    search = None,
                    hint = aHints ++ bHints))
      case (a@Select(Distinctiveness.Indistinct, aSelect, aFrom, aWhere, aGroup, aHaving, aOrder, None, None, None, aHints),
            b@Select(Distinctiveness.Indistinct, bSelect, _oldA, bWhere, Nil, None, bOrder, bLim, bOff, None, bHints)) if a.isAggregated || a.isWindowed =>
        // an non-aggregate on an aggregate
        System.err.println("MERGE: NON-AGGREGATE ON AGGREGATE")
        Some(Select(distinctiveness = Distinctiveness.Indistinct,
                    selectList = mergeSelection(fromLabel, a.selectedExprs, bSelect),
                    from = aFrom,
                    where = aWhere,
                    groupBy = aGroup,
                    having = mergeWhereLike(fromLabel, a.selectedExprs, aHaving, bWhere),
                    orderBy = mergeOrderBy(fromLabel, a.selectedExprs, aOrder, bOrder),
                    limit = bLim,
                    offset = bOff,
                    search = None,
                    hint = aHints ++ bHints))
      case _ =>
        None
    }

  private def mergeDistinct(
    aTable: TableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]],
    b: Distinctiveness[CT, CV]
  ): Distinctiveness[CT, CV] =
    b match {
      case Distinctiveness.Indistinct => Distinctiveness.Indistinct
      case Distinctiveness.FullyDistinct => Distinctiveness.FullyDistinct
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(replaceRefs(aTable, aColumns, _)))
    }

  private def mergeSelection(
    aTable: TableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]],
    b: OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]]
  ): OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]] =
    b.withValuesMapped { bExpr =>
      bExpr.copy(expr = replaceRefs(aTable, aColumns, bExpr.expr))
    }

  private def mergeGroupBy(aTable: TableLabel, aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]], gb: Seq[Expr[CT, CV]]): Seq[Expr[CT, CV]] = {
    gb.map(replaceRefs(aTable, aColumns, _))
  }

  private def mergeOrderBy(
    aTable: TableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]],
    obA: Seq[OrderBy[CT, CV]],
    obB: Seq[OrderBy[CT, CV]]
  ): Seq[OrderBy[CT, CV]] =
    obB.map { ob => ob.copy(expr = replaceRefs(aTable, aColumns, ob.expr)) } ++ obA

  private def mergeWhereLike(
    aTable: TableLabel,
    aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]],
    a: Option[Expr[CT, CV]],
    b: Option[Expr[CT, CV]]
  ): Option[Expr[CT, CV]] =
    (a, b) match {
      case (None, None) => None
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(replaceRefs(aTable, aColumns, b))
      case (Some(a), Some(b)) => Some(FunctionCall(and, Seq(a, replaceRefs(aTable, aColumns, b)))(NoPosition, NoPosition))
    }

  private def replaceRefs(aTable: TableLabel, aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]], b: Expr[CT, CV]) =
    new ReplaceRefs(aTable, aColumns).go(b)

  private class ReplaceRefs(aTable: TableLabel, aColumns: OrderedMap[AutoColumnLabel, Expr[CT, CV]]) {
    def go(b: Expr[CT, CV]): Expr[CT, CV] =
      b match {
        case Column(`aTable`, c : AutoColumnLabel, t) =>
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
