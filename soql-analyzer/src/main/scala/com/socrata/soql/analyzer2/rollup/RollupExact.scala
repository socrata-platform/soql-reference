package com.socrata.soql.analyzer2.rollup

import scala.collection.compat._

import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.{OrderedMap, NonEmptySeq}

object RollupExact {
  private val log = LoggerFactory.getLogger(classOf[RollupExact[_]])

  private final case class MergeContext(
    val isNonAggregate: Boolean,
    val isCoarseningGroup: Boolean,
    val isFirstAggregate: Boolean
  ) {
    if(isCoarseningGroup || isFirstAggregate) {
      assert(!isNonAggregate)
    }
  }
  private val NonAggregatedOnNonAggregated = MergeContext(isNonAggregate = true, isCoarseningGroup = false, isFirstAggregate = false)
  private val AggregatedOnNonAggregated = MergeContext(isNonAggregate = false, isCoarseningGroup = true, isFirstAggregate = true)
  private val AggregatedOnSameGroup = MergeContext(isNonAggregate = false, isCoarseningGroup = false, isFirstAggregate = false)
  private val AggregatedOnDifferentGroup = MergeContext(isNonAggregate = false, isCoarseningGroup = true, isFirstAggregate = false)
}

class RollupExact[MT <: MetaTypes](
  semigroupRewriter: SemigroupRewriter[MT],
  adHocRewriter: AdHocRewriter[MT],
  functionSubset: FunctionSubset[MT],
  functionSplitter: FunctionSplitter[MT],
  splitAnd: SplitAnd[MT],
  stringifier: Stringifier[MT]
) extends ((Select[MT], RollupInfo[MT, _], LabelProvider) => Option[Statement[MT]]) with StatementUniverse[MT] {
  import RollupExact._

  private type IsoState = IsomorphismState.View[MT]

  // see if there's a rollup that can be used to answer _this_ select
  // (not any sub-parts of the select!).  This needs to produce a
  // statement with the same output schema (in terms of column labels
  // and types) as the given select.
  def apply(select: Select, rollupInfo: RollupInfo[MT, _], labelProvider: LabelProvider): Option[Statement] = {
    if(select.hint(SelectHint.NoRollup)) {
      log.debug("Bailing because query asked us not to roll it up")
      return None
    }

    val candidate = rollupInfo.statement match {
      case sel: Select =>
        sel
      case stmt =>
        log.debug("Bailing because rollup is not a Select:\n  {}", stringifier.statement(stmt).indent(2))
        return None
    }

    log.debug(
      "Attempting to rewrite:\n  QUERY:\n    {}\n  ROLLUP:\n    {}",
      stringifier.statement(select).indent(4): Any,
      stringifier.statement(rollupInfo.statement).indent(4)
    )

    val isoState = select.from.verticalSliceOf(candidate.from).getOrElse {
      log.debug("Bailing because the rollup's From is not a vertical slice of the query's From")
      return None
    }

    val rewriteInTerms = new RewriteInTerms(isoState, candidate.selectList, rollupInfo.from(labelProvider))

    (select.isAggregated, candidate.isAggregated) match {
      case (false, false) =>
        rewriteUnaggregatedOnUnaggregated(select, candidate, rewriteInTerms)
      case (true, false) =>
        rewriteAggregatedOnUnaggregated(select, candidate, rewriteInTerms)
      case (false, true) =>
        log.debug("Bailing because cannot rewrite an un-aggregated query in terms of an aggregated one")
        None
      case (true, true) =>
        rewriteAggregatedOnAggregated(select, candidate, rewriteInTerms)
    }
  }

  private def rewriteUnaggregatedOnUnaggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    log.debug("attempting to rewrite an unaggregated query in terms of an unaggregated candidate")

    assert(!select.isAggregated)
    assert(!candidate.isAggregated)
    assert(select.groupBy.isEmpty)
    assert(select.having.isEmpty)
    assert(candidate.groupBy.isEmpty)
    assert(candidate.having.isEmpty)

    val Select(
      sDistinctiveness,
      sSelectList,
      sFrom,
      sWhere,
      _sGroupBy,
      _sHaving,
      sOrderBy,
      sLimit,
      sOffset,
      sSearch,
      sHint
    ) = select

    val Select(
      cDistinctiveness,
      cSelectList,
      cFrom,
      cWhere,
      _cGroupBy,
      _cHaving,
      cOrderBy,
      cLimit,
      cOffset,
      cSearch,
      _cHint
    ) = candidate

    rewriteOneToOne(
      select.isWindowed,
      sDistinctiveness,
      sSelectList,
      sFrom,
      sWhere,
      sOrderBy,
      sLimit,
      sOffset,
      sSearch,
      sHint,

      candidate.isWindowed,
      cDistinctiveness,
      cSelectList,
      cFrom,
      cWhere,
      cOrderBy,
      cLimit,
      cOffset,
      cSearch,

      rewriteInTerms,
      "WHERE",
      NonAggregatedOnNonAggregated
    )
  }

  // This is used for places where we're not grouping the rollup
  // (i.e., the queries are not grouped or both are grouped the same
  // way).
  private def rewriteOneToOne(
    sIsWindowed: Boolean,
    sDistinctiveness: Distinctiveness,
    sSelectList: OrderedMap[AutoColumnLabel, NamedExpr],
    sFrom: From,
    sWhereish: Option[Expr],
    sOrderBy: Seq[OrderBy],
    sLimit: Option[BigInt],
    sOffset: Option[BigInt],
    sSearch: Option[String],
    sHint: Set[SelectHint],

    cIsWindowed: Boolean,
    cDistinctiveness: Distinctiveness,
    cSelectList: OrderedMap[AutoColumnLabel, NamedExpr],
    cFrom: From,
    cWhereish: Option[Expr],
    cOrderBy: Seq[OrderBy],
    cLimit: Option[BigInt],
    cOffset: Option[BigInt],
    cSearch: Option[String],

    rewriteInTerms: RewriteInTerms,
    whereName: String,
    mergeContext: MergeContext
  ): Option[Select] = {
    // * the candidate must be indistinct, or its DISTINCT clause         ✓
    //   must match (requires WHERE and ORDER isomorphism, and
    //   if fully distinct, select list isomorphism)
    // * all expressions in select list, ORDER BY must be                 ✓
    //   expressible in terms of the output columns of candidate
    // * WHERE must be a supersequence of the candidate's, and the        ✓
    //   parts that aren't must be expressible in terms of the output
    //   columns of candidate
    // * WHERE must be isomorphic if the candidate is windowed and the    ✓ (kinda, see "This isn't quite right!" below)
    //   windowed expressions are used in this select.
    // * SEARCH must not exist on either select                           ✓
    // * If LIMIT and/or OFFSET is specified on the candidate, then
    //   the ORDER BY must be no more constraining than the
    //   candidate's, and the specified window must occur within what
    //   the candidate has

    if(sSearch.isDefined || cSearch.isDefined) {
      log.debug("Bailing because SEARCH makes rollups bad")
      return None
    }


    lazy val whereishIsIsomorphic: Boolean =
      sWhereish.isDefined == cWhereish.isDefined &&
        sWhereish.lazyZip(cWhereish).forall { (a, b) => a.isIsomorphic(b, rewriteInTerms.isoState) }

    val newWhere: Option[Expr] =
      (sWhereish, cWhereish) match {
        case (sWhere, None) =>
          sWhere.mapFallibly(rewriteInTerms.rewrite(_, mergeContext)).getOrElse {
            log.debug(s"Bailing because couldn't rewrite $whereName in terms of candidate output columns")
            return None
          }

        case (Some(sWhere), Some(cWhere)) =>
          if(whereishIsIsomorphic) {
            None
          } else {
            combineAnd(sWhere, cWhere, rewriteInTerms, mergeContext).getOrElse {
              log.debug(s"Bailing because couldn't express the query's $whereName in terms of the candidate's $whereName")
              return None
            }
          }

        case (None, Some(_)) =>
          log.debug(s"Bailing because the candidate has a $whereName clause and the query does not")
          return None
      }

    // If the candidate's ORDER BY matters, it must be at least as
    // constrained as the select's.
    lazy val orderByIsIsomorphicUpToSelectsRequirements: Boolean =
      sOrderBy.length <= cOrderBy.length &&
        sOrderBy.lazyZip(cOrderBy).forall { (a, b) => a.isIsomorphic(b, rewriteInTerms.isoState) }

    if(sIsWindowed && cIsWindowed && !whereishIsIsomorphic) {
      // This isn't quite right!  We only need to bail here if one of
      // the candidate's windowed expressions is actually used in the
      // query.
      log.debug(s"Bailing because the candidate windowed but ${whereName}s are not isomorphic")
      return None
    }

    val newOrderBy: Seq[OrderBy] = sOrderBy.mapFallibly(rewriteInTerms.rewriteOrderBy(_, mergeContext)).getOrElse {
      log.debug("Bailing because couldn't rewrite ORDER BY in terms of candidate output columns")
      return None
    }

    (sDistinctiveness, cDistinctiveness) match {
      case (_, Distinctiveness.Indistinct()) =>
        // ok, we can work with this
      case (Distinctiveness.FullyDistinct(), Distinctiveness.FullyDistinct()) =>
        // we need to select the exact same columns
        if(!sameSelectList(sSelectList, cSelectList, rewriteInTerms.isoState)) {
          log.debug("Bailing because DISTINCT but different select lists")
          return None
        }

        if(!whereishIsIsomorphic) {
          log.debug(s"Bailing because DISTINCT but $whereName is not isomorphic")
          return None
        }
      case (Distinctiveness.On(sOn), Distinctiveness.On(cOn)) =>
        if(sOn.length != cOn.length || sOn.lazyZip(cOn).exists { (s, c) => !s.isIsomorphic(c, rewriteInTerms.isoState) }) {
          log.debug("Bailing because of DISTINCT ON mismatch")
          return None
        }

        // ORDER BY needs to be isomorphic because DISTINCT ON picks
        // the first row
        if(!whereishIsIsomorphic || !orderByIsIsomorphicUpToSelectsRequirements) {
          log.debug(s"Bailing because DISTINCT ON but $whereName or ORDER BY are not isomorphic")
          return None
        }
      case _ =>
        log.debug("Bailing because of a distinctiveness mismatch")
        return None
      }
    val newDistinctiveness = rewriteDistinctiveness(sDistinctiveness, rewriteInTerms, mergeContext).getOrElse {
      log.debug("Bailing because failed to rewrite distinctiveness")
      return None
    }

    val newSelectList: OrderedMap[AutoColumnLabel, NamedExpr] =
      OrderedMap() ++ sSelectList.iterator.mapFallibly(rewriteInTerms.rewriteSelectListEntry(_, mergeContext)).getOrElse {
        log.debug("Bailing because failed to rewrite the select list")
        return None
      }

    val (newLimit, newOffset) =
      combineLimitOffset(sLimit, sOffset, cLimit, cOffset, orderByIsIsomorphicUpToSelectsRequirements).getOrElse {
        return None
      }

    Some(
      Select(
        distinctiveness = newDistinctiveness,
        selectList = newSelectList,
        from = rewriteInTerms.newFrom,
        where = newWhere,
        groupBy = Nil,
        having = None,
        orderBy = newOrderBy,
        limit = newLimit,
        offset = newOffset,
        search = None,
        hint = sHint
      )
    )
  }

  private def sameSelectList(s: OrderedMap[AutoColumnLabel, NamedExpr], c: OrderedMap[AutoColumnLabel, NamedExpr], isoState: IsoState): Boolean = {
    // we don't care about order or duplicates, but we do care that
    // all exprs in s are isomorphic to exprs in c, and vice-versa.
    val sExprs = s.values.map(_.expr).toVector
    val cExprs = c.values.map(_.expr).toVector

    sameExprsIgnoringOrder(sExprs, cExprs, isoState)
  }

  private def sameExprsIgnoringOrder(s: Seq[Expr], c: Seq[Expr], isoState: IsoState): Boolean = {
    def isSubset(as: Seq[Expr], bs: Seq[Expr], isoState: IsoState): Boolean = {
      as.forall { a =>
        bs.exists { b => a.isIsomorphic(b, isoState) }
      }
    }

    isSubset(s, c, isoState) && isSubset(c, s, isoState.reverse)
  }

  private def rewriteDistinctiveness(distinct: Distinctiveness, rewriteInTerms: RewriteInTerms, rollupContext: MergeContext): Option[Distinctiveness] = {
    distinct match {
      case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() =>
        Some(distinct)
      case Distinctiveness.On(exprs) =>
        exprs.mapFallibly(rewriteInTerms.rewrite(_, rollupContext)).map(Distinctiveness.On(_))
    }
  }

  private def rewriteAggregatedOnUnaggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    // * all expressions in the select list, GROUP BY, HAVING,            ✓
    //   and ORDER BY must be expressible in terms of the output
    //   columns of the candidate
    // * WHERE must be a supersequence of the candidate's, and the        ✓
    //   parts that aren't must be expressible in terms of the output
    //   columns of candidate
    // * SEARCH must not exist on either select                           ✓
    // * Neither LIMIT nor OFFSET may exist on the candidate              ✓
    // * candidate must not have a DISTINCT or DISTINCT ON                ✓

    log.debug("attempting to rewrite an aggregated query in terms of an unaggregated candidate")

    assert(select.isAggregated)
    assert(!candidate.isAggregated)
    assert(candidate.groupBy.isEmpty)
    assert(candidate.having.isEmpty)

    if(select.search.isDefined || candidate.search.isDefined) {
      log.debug("Bailing because SEARCH makes rollups bad")
      return None
    }

    if(candidate.limit.isDefined || candidate.offset.isDefined) {
      log.debug("Bailing because the candidate has a LIMIT/OFFSET")
      return None
    }

    candidate.distinctiveness match {
      case Distinctiveness.Indistinct() =>
        // ok
      case Distinctiveness.FullyDistinct() | Distinctiveness.On(_) =>
        log.debug("Bailing because the candidate has a DISTINCT clause")
        return None
    }
    val newDistinctiveness = rewriteDistinctiveness(select.distinctiveness, rewriteInTerms, AggregatedOnNonAggregated).getOrElse {
      log.debug("Bailing because failed to rewrite the query's DISTINCT clause")
      return None
    }

    val newWhere: Option[Expr] =
      candidate.where match {
        case Some(cWhere) =>
          select.where match {
            case Some(sWhere) =>
              combineAnd(sWhere, cWhere, rewriteInTerms, AggregatedOnNonAggregated).getOrElse {
                log.debug("Bailing because couldn't express the query's WHERE in terms of the candidate's WHERE")
                return None
              }
            case None =>
              log.debug("Bailing because candidate had a WHERE and the query does not")
              return None
          }
        case None =>
          select.where.mapFallibly(rewriteInTerms.rewrite(_, AggregatedOnNonAggregated)).getOrElse {
            log.debug("Bailing because unable to rewrite the WHERE in terms of the candidate's output columns")
            return None
          }
      }

    val newGroupBy = select.groupBy.mapFallibly(rewriteInTerms.rewrite(_, AggregatedOnNonAggregated)).getOrElse {
      log.debug("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
      return None
    }
    val newHaving = select.having.mapFallibly(rewriteInTerms.rewrite(_, AggregatedOnNonAggregated)).getOrElse {
      log.debug("Bailing because unable to rewrite the HAVING in terms of the candidate's output columns")
      return None
    }
    val newOrderBy = select.orderBy.mapFallibly(rewriteInTerms.rewriteOrderBy(_, AggregatedOnNonAggregated)).getOrElse {
      log.debug("Bailing because unable to rewrite the ORDER BY in terms of the candidate's output columns")
      return None
    }
    val newSelectList = OrderedMap() ++ select.selectList.iterator.mapFallibly(rewriteInTerms.rewriteSelectListEntry(_, AggregatedOnNonAggregated)).getOrElse {
      log.debug("Bailing because unable to rewrite the select list in terms of the candidate's output columns")
      return None
    }

    Some(
      Select(
        distinctiveness = newDistinctiveness,
        selectList = newSelectList,
        from = rewriteInTerms.newFrom,
        where = newWhere,
        groupBy = newGroupBy,
        having = newHaving,
        orderBy = newOrderBy,
        limit = select.limit,
        offset = select.offset,
        search = None,
        hint = select.hint
      )
    )
  }

  private def rewriteAggregatedOnAggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    log.debug("attempting to rewrite an aggregated query in terms of an aggregated candidate")

    assert(select.isAggregated)
    assert(candidate.isAggregated)

    val extractedWhere: Option[Expr] =
      (select.where, candidate.where) match {
        case (None, None) =>
          None
        case (Some(sWhere), Some(cWhere)) =>
          // this is a little stricter than it could be, but making it
          // less strict would entail knowing more about functions
          // mean.  For example, if the candidate has `where x > 5`
          // and the query has `where x = 10` then these where clauses
          // are in fact actually compatible, but knowing that depends
          // on knowing the exact meaning of the op$> and op$=
          // functions and being able to tell when one boolean
          // expression is true for a superset of where another
          // boolean expression is.
          val sWhereSplit = splitAnd.split(sWhere)
          val cWhereSplit = splitAnd.split(cWhere)
          val sLeftover = exprSubsequence(sWhereSplit, cWhereSplit, rewriteInTerms).getOrElse {
            log.debug("Bailing because WHERE was not a supersequence")
            return None
          }
          NonEmptySeq.fromSeq(sLeftover).
            map(splitAnd.merge)
        case (Some(sWhere), None) =>
          Some(sWhere)
        case (None, Some(_)) =>
          log.debug("Bailing because the candidate had a WHERE and the query didn't")
          return None
      }

    val updatedSelect = select.copy(where = extractedWhere)

    if(sameExprsIgnoringOrder(select.groupBy, candidate.groupBy, rewriteInTerms.isoState)) {
      rewriteAggregatedOnAggregatedSameGroupBy(updatedSelect, candidate, rewriteInTerms)
    } else {
      rewriteAggregatedOnAggregatedDifferentGroupBy(updatedSelect, candidate, rewriteInTerms)
    }
  }

  private def rewriteAggregatedOnAggregatedDifferentGroupBy(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    // * the expressions in the select's GROUP BY must be expressible     ✓
    //   in terms of the output columns of the candidate¹
    // * WHERE must be more restrictive than the candidate's WHERE        ✓
    //   and it must be expressible in terms of the output columns
    //   of candidate
    // * all expressions in select list and ORDER BY must be              ✓
    //   expressible in terms of the output columns of candidate
    //   under monoidal combination
    // * candidate must not have a HAVING                                 ✓
    // * SEARCH must not exist on either select                           ✓
    // * Neither LIMIT nor OFFSET may exist on the candidate              ✓
    // * candidate must not have a DISTINCT or DISTINCT ON                ✓
    //
    // ¹ This works because in order to be a GROUP BY expression it
    // must not itself contain any aggregate or window functions.
    // Therefore, any expressions which we _can_ rewrite do so in
    // terms of non-aggregate/window select columns - which means
    // they're either constant² or the grouped epxressions themselves.
    // ² This absolutely depends on rollups being forbidden from
    // being defined in terms of impure functions!  Currently our only
    // impure functions are get_context and get_utc_date, both of
    // which are forbidden in rollups, but this is a point of fragility
    // which must be kept in mind while adding functions.

    if(select.search.isDefined || candidate.search.isDefined) {
      log.debug("Bailing because SEARCH makes rollups bad")
      return None
    }

    if(candidate.limit.isDefined || candidate.offset.isDefined) {
      log.debug("Bailing because the candidate has a LIMIT/OFFSET")
      return None
    }

    candidate.distinctiveness match {
      case Distinctiveness.Indistinct() =>
        // ok
      case Distinctiveness.FullyDistinct() | Distinctiveness.On(_) =>
        log.debug("Bailing because the candidate has a DISTINCT clause")
        return None
    }

    if(candidate.having.isDefined) {
      log.debug("Bailing because the candidate has a HAVING")
      return None
    }

    val newDistinctiveness = rewriteDistinctiveness(select.distinctiveness, rewriteInTerms, rollupContext = AggregatedOnDifferentGroup).getOrElse {
      log.debug("Bailing because failed to rewrite the DISTINCT clause")
      return None
    }

    val newSelectList: OrderedMap[AutoColumnLabel, NamedExpr] =
      OrderedMap() ++ select.selectList.iterator.mapFallibly(rewriteInTerms.rewriteSelectListEntry(_, rollupContext = AggregatedOnDifferentGroup)).getOrElse {
        log.debug("Bailing because failed to rewrite the select list")
        return None
      }

    val newWhere =
      select.where.mapFallibly(rewriteInTerms.rewrite(_, rollupContext = AggregatedOnDifferentGroup)).getOrElse {
        log.debug("Bailing because unable to rewrite the WHERE in terms of the candidate's output columns")
        return None
      }

    val newGroupBy =
      select.groupBy.mapFallibly(rewriteInTerms.rewrite(_, rollupContext = AggregatedOnDifferentGroup)).getOrElse {
        log.debug("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
        return None
      }

    val newHaving =
      select.having.mapFallibly(rewriteInTerms.rewrite(_, rollupContext = AggregatedOnDifferentGroup)).getOrElse {
        log.debug("Bailing because unable to rewrite the HAVING in terms of the candidate's output columns")
        return None
      }

    val newOrderBy = select.orderBy.mapFallibly(rewriteInTerms.rewriteOrderBy(_, rollupContext = AggregatedOnDifferentGroup)).getOrElse {
      log.debug("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
      return None
    }

    Some(
      Select(
        distinctiveness = newDistinctiveness,
        selectList = newSelectList,
        from = rewriteInTerms.newFrom,
        where = newWhere,
        groupBy = newGroupBy,
        having = newHaving,
        orderBy = newOrderBy,
        limit = select.limit,
        offset = select.offset,
        search = None,
        hint = select.hint
      )
    )
  }

  private def rewriteAggregatedOnAggregatedSameGroupBy(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    // This is much more like unaggregated-on-unaggregated except that
    // we'll be promoting the select's HAVING clause into the new
    // query's WHERE.

    // In fact, we can actually do this in terms of the other!

    val Select(
      sDistinctiveness,
      sSelectList,
      sFrom,
      sWhere,
      _sGroupBy,
      sHaving,
      sOrderBy,
      sLimit,
      sOffset,
      sSearch,
      sHint
    ) = select

    val Select(
      cDistinctiveness,
      cSelectList,
      cFrom,
      _cWhere,
      _cGroupBy,
      cHaving,
      cOrderBy,
      cLimit,
      cOffset,
      cSearch,
      _cHint
    ) = candidate

    val newSHaving =
      (sWhere, sHaving) match {
        case (w, None) => w
        case (None, h) => h
        case (Some(w), Some(h)) => Some(splitAnd.merge(NonEmptySeq(w, List(h))))
      }

    rewriteOneToOne(
      select.isWindowed,
      sDistinctiveness,
      sSelectList,
      sFrom,
      newSHaving,
      sOrderBy,
      sLimit,
      sOffset,
      sSearch,
      sHint,

      candidate.isWindowed,
      cDistinctiveness,
      cSelectList,
      cFrom,
      cHaving,
      cOrderBy,
      cLimit,
      cOffset,
      cSearch,

      rewriteInTerms,
      "HAVING",
      AggregatedOnSameGroup
    )
  }

  private def combineLimitOffset(
    sLimit: Option[BigInt],
    sOffset: Option[BigInt],
    cLimit: Option[BigInt],
    cOffset: Option[BigInt],
    orderByIsIsomorphicUpToSelectsRequirements: =>Boolean
  ): Option[(Option[BigInt], Option[BigInt])] = {
    (cLimit, cOffset) match {
      case (None, None) =>
        Some((sLimit, sOffset))

      case (maybeLim, maybeOff) =>
        if(!orderByIsIsomorphicUpToSelectsRequirements) {
          log.debug("Bailing because candidate has limit/offset but ORDER BYs are not isomorphic")
          return None
        }

        val requestedOffset = sOffset.getOrElse(BigInt(0))
        val existingOffset = maybeOff.getOrElse(BigInt(0))

        if(requestedOffset < existingOffset) {
          log.debug("Bailing because candidate has limit/offset but the query's window precedes it")
          return None
        }

        val newOffset = requestedOffset + existingOffset

        (sLimit, maybeLim) match {
          case (None, None) =>
            if(newOffset == 0) {
              Some((None, None))
            } else {
              Some((None, Some(newOffset)))
            }
          case (None, Some(_)) =>
            log.debug("Bailing because candidate has a limit but the query does not")
            return None
          case (sLimit@Some(_), None) =>
            Some((sLimit, Some(newOffset)))
          case (Some(sLimit), Some(cLimit)) =>
            if(newOffset + sLimit > cLimit) {
              log.debug("Bailing because candiate has a limit and the query's extends beyond its end")
              return None
            }

            Some((Some(sLimit), Some(newOffset)))
        }
    }
  }

  // if "b" is a subsequence of "a", returns the elements of "a" not in "b"
  private def exprSubsequence(as: NonEmptySeq[Expr], bs: NonEmptySeq[Expr], rewriteInTerms: RewriteInTerms): Option[Seq[Expr]] = {
    var remainingBs = bs.toList
    val result = Vector.newBuilder[Expr]
    for(a <- as) {
      remainingBs match {
        case b :: bRemaining =>
          if(a.isIsomorphic(b, rewriteInTerms.isoState)) {
            remainingBs = bRemaining
          } else {
            result += a
          }
        case Nil =>
          result += a
      }
    }

    if(remainingBs.nonEmpty) {
      None
    } else {
      Some(result.result())
    }
  }

  private def combineAnd(sExpr: Expr, cExpr: Expr, rewriteInTerms: RewriteInTerms, rollupContext: MergeContext): Option[Option[Expr]] = {
    val sSplit = splitAnd.split(sExpr)
    val cSplit = splitAnd.split(cExpr)

    // ok, this is a little subtle.  We have two lists of AND clauses,
    // and we want to make sure that cSplit is a subseqence of sSplit,
    // to preserve short-circuiting.
    val newTerms = exprSubsequence(sSplit, cSplit, rewriteInTerms).getOrElse {
      log.debug("Leftover candidate clauses after scan")
      return None
    }

    NonEmptySeq.fromSeq(newTerms).map(splitAnd.merge).mapFallibly(rewriteInTerms.rewrite(_, rollupContext))
  }

  private class RewriteInTerms(
    val isoState: IsoState,
    candidateSelectList: OrderedMap[AutoColumnLabel, NamedExpr],
    val newFrom: FromTable
  ) {
    // Check that our candidate select list and table info are in
    // sync, and build a map from AutoColumnLabel to
    // DatabaseColumnName so when we're rewriting exprs we can fill in
    // with references to PhysicalColumns
    assert(candidateSelectList.size == newFrom.schema.length)
    private val columnLabelMap: Map[AutoColumnLabel, DatabaseColumnName] =
      candidateSelectList.lazyZip(newFrom.columns).map { case ((label, namedExpr), (databaseColumnName, nameEntry)) =>
        assert(namedExpr.name == nameEntry.name && namedExpr.expr.typ == nameEntry.typ)

        label -> databaseColumnName
      }.toMap

    def rewrite(sExpr: Expr, rollupContext: MergeContext): Option[Expr] =
      doNonAdHocRewrite(sExpr, rollupContext = rollupContext) orElse {
        adHocRewriter(sExpr).findMap(doNonAdHocRewrite(_, rollupContext))
      }

    private def doNonAdHocRewrite(sExpr: Expr, rollupContext: MergeContext): Option[Expr] =
      rewriteSimple(sExpr, rollupContext).orElse { rewriteCompound(sExpr, rollupContext) }

    // This exists specifically to handle the case of "avg" but in
    // principle could also be used for other things?  Anyway,
    // this rewrites a(x, y, z) into combiner(b(x, y, z), c(x, y, z), ...)
    // and then tries to rewrite that form.
    private def rewriteCompound(sExpr: Expr, rollupContext: MergeContext): Option[Expr] =
      sExpr match {
        case fc@FunctionCall(func, args) =>
          functionSplitter(func).flatMap { case (combiner, inputFuncs) =>
            rewrite(
              FunctionCall(
                combiner,
                inputFuncs.map { inputFunc =>
                  FunctionCall(inputFunc, args)(fc.position)
                }
              )(fc.position),
              rollupContext
            )
          }

        case afc@AggregateFunctionCall(func, args, distinct, filter) =>
          functionSplitter(func).flatMap { case (combiner, inputFuncs) =>
            rewrite(
              FunctionCall(
                combiner,
                inputFuncs.map { inputFunc =>
                  AggregateFunctionCall(inputFunc, args, distinct, filter)(afc.position)
                }
              )(afc.position),
              rollupContext
            )
          }

        case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
          functionSplitter(func).flatMap { case (combiner, inputFuncs) =>
            rewrite(
              FunctionCall(
                combiner,
                inputFuncs.map { inputFunc =>
                  WindowedFunctionCall(inputFunc, args, filter, partitionBy, orderBy, frame)(wfc.position)
                }
              )(wfc.position),
              rollupContext
            )
          }

        case _ =>
          None
      }

    private def rewriteSimple(sExpr: Expr, rollupContext: MergeContext): Option[Expr] =
      candidateSelectList.iterator.findMap { case (label, ne) =>
        if(sExpr.isIsomorphic(ne.expr, isoState)) {
          Some((label, ne, identity[Expr] _))
        } else {
          None
        }
      }.orElse(
        candidateSelectList.iterator.findMap { case (label, ne) =>
          functionSubset(sExpr, ne.expr, isoState).map((label, ne, _))
        }
      ) match {
        case Some((selectedColumn, rollupExpr@NamedExpr(AggregateFunctionCall(func, args, false, None), _name, _hint, _isSynthetic), functionExtract)) if rollupContext.isCoarseningGroup =>
          assert(rollupExpr.typ == sExpr.typ)
          semigroupRewriter(func).map { merger =>
            functionExtract(merger(PhysicalColumn[MT](newFrom.label, newFrom.tableName, columnLabelMap(selectedColumn), rollupExpr.typ)(sExpr.position.asAtomic)))
          }
        case Some((selectedColumn, NamedExpr(_ : AggregateFunctionCall, _name, _hint, _isSynthetic), _)) if rollupContext.isCoarseningGroup =>
          log.debug("can't rewrite, the aggregate has a 'distinct' or 'filter'")
          None
        case Some((selectedColumn, NamedExpr(_ : WindowedFunctionCall, _name, _hint, _isSynthetic), _)) if rollupContext.isCoarseningGroup =>
          log.debug("can't rewrite, windowed function call")
          None
        case Some((selectedColumn, NamedExpr(funcall : FunctionCall, _name, _hint, _isSynthetic), _)) if (funcall.isAggregated || funcall.isWindowed) && rollupContext.isCoarseningGroup =>
          log.debug("can't rewrite, nontrivial nested aggregate")
          None
        case Some((selectedColumn, rollupExpr, functionExtract)) =>
          val result = functionExtract(PhysicalColumn[MT](newFrom.label, newFrom.tableName, columnLabelMap(selectedColumn), rollupExpr.typ)(sExpr.position.asAtomic))
          assert(result.typ == sExpr.typ)
          Some(result)
        case None =>
          sExpr match {
            case lit: LiteralValue => Some(lit)
            case nul: NullLiteral => Some(nul)
            case slr: SelectListReference => throw new AssertionError("Rollups should never see a SelectListReference")
            case fc@FunctionCall(func, args) =>
              args.mapFallibly(rewrite(_, rollupContext)).map { newArgs =>
                FunctionCall(func, newArgs)(fc.position)
              }
            case afc@AggregateFunctionCall(func, args, distinct, filter) if rollupContext.isFirstAggregate =>
              for {
                newArgs <- args.mapFallibly(rewrite(_, rollupContext))
                newFilter <- filter.mapFallibly(rewrite(_, rollupContext))
              } yield {
                AggregateFunctionCall(func, newArgs, distinct, newFilter)(afc.position)
              }
            case afc@AggregateFunctionCall(func, args, distinct, filter) if semigroupRewriter.isSemilattice(func) =>
              val newContext = rollupContext.copy(isCoarseningGroup = false)
              for {
                newArgs <- args.mapFallibly(rewrite(_, newContext))
                newFilter <- filter.mapFallibly(rewrite(_, newContext))
              } yield {
                AggregateFunctionCall(func, newArgs, distinct, newFilter)(afc.position)
              }
            case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) if rollupContext.isNonAggregate || rollupContext.isFirstAggregate =>
              for {
                newArgs <- args.mapFallibly(rewrite(_, rollupContext))
                newFilter <- filter.mapFallibly(rewrite(_, rollupContext))
                newPartitionBy <- partitionBy.mapFallibly(rewrite(_, rollupContext))
                newOrderBy <- orderBy.mapFallibly(rewriteOrderBy(_, rollupContext))
              } yield {
                WindowedFunctionCall(func, args, newFilter, newPartitionBy, newOrderBy, frame)(wfc.position)
              }
            case other =>
              None
          }
      }

    def rewriteOrderBy(sOrderBy: OrderBy, rollupContext: MergeContext): Option[OrderBy] =
      rewrite(sOrderBy.expr, rollupContext).map { newExpr => sOrderBy.copy(expr = newExpr) }

    def rewriteNamedExpr(sNamedExpr: NamedExpr, rollupContext: MergeContext): Option[NamedExpr] =
      rewrite(sNamedExpr.expr, rollupContext).map { newExpr => sNamedExpr.copy(expr = newExpr) }

    def rewriteSelectListEntry(sSelectListEntry: (AutoColumnLabel, NamedExpr), rollupContext: MergeContext): Option[(AutoColumnLabel, NamedExpr)] = {
      val (sLabel, sNamedExpr) = sSelectListEntry
      rewriteNamedExpr(sNamedExpr, rollupContext).map { newExpr => sLabel -> newExpr }
    }
  }
}
