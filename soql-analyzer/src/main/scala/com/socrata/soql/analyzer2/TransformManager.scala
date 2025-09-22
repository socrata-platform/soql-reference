package com.socrata.soql.analyzer2

import scala.collection.immutable.ListSet
import scala.collection.compat._

import com.rojoma.json.v3.util.JsonUtil
import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2._
import com.socrata.soql.util.LazyToString

object TransformManager {
  private val log = LoggerFactory.getLogger(classOf[TransformManager[_, _]])
}

// The transform manager applies rewrite passes and rollups
class TransformManager[MT <: MetaTypes, RollupId](
  rollupExact: rollup.RollupExact[MT],
  rewritePassHelpers: rewrite.RewritePassHelpers[MT],
  stringifier: Stringifier[MT]
) {
  import TransformManager._

  def apply(
    untransformedAnalysis: SoQLAnalysis[MT],
    rollups: Seq[rollup.RollupInfo[MT, RollupId]],
    passes: Seq[Seq[rewrite.Pass]]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]): Vector[(SoQLAnalysis[MT], Set[RollupId])] = {
    log.debug("Rewriting query:\n  {}", stringifier.statement(untransformedAnalysis.statement).indent(2))

    // Rollups that aren't semantics-preserving need to happen
    // _before_ rollups because otherwise there might not be enough
    // information left to carry them out.
    val preRollupPasses = passes.reverse.dropWhile { chunk => chunk.forall(isRollupSafe) }.reverse
    val postRollupPasses = passes.drop(preRollupPasses.length)

    log.debug("Pre-rollup passes: {}", LazyToString(JsonUtil.renderJson(preRollupPasses, pretty=false)))
    log.debug("Post-rollup passes: {}", LazyToString(JsonUtil.renderJson(postRollupPasses, pretty=false)))

    // What we're doing here is starting with an un-rewritten analysis
    // and a set of rollups.  First, we try to rewrite that analysis
    // using the rollups, which gives us a set of "candidate" rewrites.
    val initialAnalysis = preRollupPasses.foldLeft(untransformedAnalysis)(_.applyPasses(_, rewritePassHelpers))
    val initialRollupCandidates = doRollup(initialAnalysis, rollups)
    log.debug("Candidate rollups:\n  {}", LazyToString(printRollups(initialRollupCandidates)).indent(2))

    // ..then, we apply the passes to the analysis _and_ each
    // candidate, and at each step try to apply rollups anew.
    val (resultAnalysis, rolledUp) =
      postRollupPasses.foldLeft(
        (initialAnalysis, initialRollupCandidates)
      ) { case ((previousAnalysis, previousRollupCandidates), passes) =>
          log.debug("Applying {} before more rollups", LazyToString(JsonUtil.renderJson(passes, pretty=false)))
          val newAnalysis =
            previousAnalysis.applyPasses(passes, rewritePassHelpers)

          val newRollupCandidates = doRollup(newAnalysis, rollups) ++
            previousRollupCandidates.flatMap { case previousCandidate =>
              val newRollupCandidateAnalysis = previousCandidate.analysis.applyPasses(passes, rewritePassHelpers)
              Vector(previousCandidate.copy(analysis = newRollupCandidateAnalysis)) ++
                doRollup(newRollupCandidateAnalysis, rollups).map { wsa =>
                  wsa.copy(rollupIds = wsa.rollupIds ++ previousCandidate.rollupIds)
                }
            }

          val filtered = newRollupCandidates.tails.flatMap { remainder =>
            if(remainder.isEmpty || remainder.tail.exists(_.analysis.statement.isIsomorphic(remainder.head.analysis.statement))) {
              None
            } else {
              Some(remainder.head)
            }
          }.toVector

          log.debug("After {}:\n  {}", passes: Any, stringifier.statement(newAnalysis.statement).indent(2))
          log.debug("Candidate rollups:\n  {}", LazyToString(printRollups(filtered)).indent(2))

          (newAnalysis, filtered)
      }

    // The result is that we get the leaves of a tree of possible
    // rewrites, where the root node is the initial analysis, and each
    // nodes children is itself with another set of passes applied,
    // plus any possible rewrites

    Vector((resultAnalysis, Set.empty[RollupId])) ++ rolledUp.map { wa =>
      (wa.analysis, wa.rollupIds)
    }
  }

  private def isRollupSafe(pass: rewrite.AnyPass) =
    pass.semanticsPreserving || !pass.deep

  private def printRollups(rollups: Iterable[WrappedSoQLAnalysis]): String =
    rollups.iterator.map { wsa => stringifier.statement(wsa.analysis.statement) }.mkString(";\n")

  private case class WrappedSoQLAnalysis(analysis: SoQLAnalysis[MT], rollupIds: Set[RollupId])

  private def doRollup(
    analysis: SoQLAnalysis[MT],
    rollups: Seq[rollup.RollupInfo[MT, RollupId]]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]): Seq[WrappedSoQLAnalysis] = {
    // ugh - modifySeq doesn't play nicely with returning additional
    // info in addition to the new analysis, so we need to pack that
    // away in a var here and then reassemble afterward.
    var rollupIdses = Seq.empty[Set[RollupId]]
    val newAnalyses =
      analysis.modifySeq { (labelProvider, statement) =>
        val rr = new rollup.RollupRewriter(labelProvider, rollupExact, rollups)
        val rewritten = rr.rollup(statement)
        rollupIdses = rewritten.map(_._2)
        rewritten.map(_._1)
      }.toVector
    newAnalyses.lazyZip(rollupIdses).map { (analysis, rollupIds) =>
      new WrappedSoQLAnalysis(analysis, rollupIds)
    }
  }
}
