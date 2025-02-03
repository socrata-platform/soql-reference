package com.socrata.soql.aliases

import scala.collection.compat._
import scala.util.parsing.input.Position
import scala.collection.mutable
import com.socrata.soql.ast._
import com.socrata.soql.exceptions.{CircularAliasDefinition, DuplicateAlias, NoSuchColumn, RepeatedException}
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName, UntypedDatasetContext}
import com.socrata.soql.collection.{OrderedMap, OrderedSet}

trait AliasAnalysis {
  type Qualifier = String
  type AnalysisContext = Map[Qualifier, UntypedDatasetContext]

  case class Analysis(expressions: OrderedMap[ColumnName, AliasAnalysis.ExpressionInfo], evaluationOrder: Seq[ColumnName])
  def apply(selection: Selection, from: Option[TableName])(implicit ctx: AnalysisContext): Analysis
}

object AliasAnalysis extends AliasAnalysis {
  sealed abstract class AliasType
  object AliasType {
    case object Implicit extends AliasType
    case object SemiExplicit extends AliasType
    case object Explicit extends AliasType
  }

  case class ExpressionInfo(expr: Expression, aliasType: AliasType)

  val log = org.slf4j.LoggerFactory.getLogger(classOf[AliasAnalysis])

  /** Validate and generate aliases for selected columns.
    *
    * @return an Analysis object which contains an ordered map from each alias to its expansion
    *         and a list of the aliases in the order they should be typechecked/evaluated.
    *
    * @throws com.socrata.soql.exceptions.RepeatedException if the same column is excepted more than once
    * @throws com.socrata.soql.exceptions.NoSuchColumn if a column not on the dataset is excepted
    * @throws com.socrata.soql.exceptions.DuplicateAlias if duplicate aliases are detected
    * @throws com.socrata.soql.exceptions.CircularAliasDefinition if an alias is defined in terms of itself
    */
  def apply(selection: Selection, from: Option[TableName] = None)(implicit ctx: AnalysisContext): Analysis = {
    log.debug("Input: {}", selection)
    val starsExpanded = expandSelection(selection, from)
    log.debug("After expanding stars: {}", starsExpanded)
    val (semiExplicit, explicitAndSemiExplicitAssigned) = assignExplicitAndSemiExplicit(starsExpanded)
    log.debug("After assigning explicit and semi-explicit: {}", explicitAndSemiExplicitAssigned)
    val allAliasesAssigned = assignImplicit(explicitAndSemiExplicitAssigned, semiExplicit)
    log.debug("After assigning implicit aliases: {}", allAliasesAssigned)
    Analysis(allAliasesAssigned, orderAliasesForEvaluation(allAliasesAssigned, semiExplicit))
  }

  /**
   * Expands the `:*` and `*` in a selection-list into an explicit list of all the
   * selections in the provided [[com.socrata.soql.ast.Selection]].
   *
   * @param selection A selection-list to desugar.
   * @return The same list with any stars expanded
   *
   * @throws com.socrata.soql.exceptions.RepeatedException if the same column is excepted more than once
   * @throws com.socrata.soql.exceptions.NoSuchColumn if a column not on the dataset is excepted
   */
  def expandSelection(selection: Selection, from: Option[TableName] = None)(implicit ctx: AnalysisContext): Seq[SelectedExpression] = {
    val defaultQualifier = from.map(_.qualifier).getOrElse(TableName.PrimaryTable.qualifier)
    val Selection(systemStar, userStars, expressions) = selection
    val ctxKeyForSystem = systemStar.flatMap(_.qualifier).getOrElse(defaultQualifier)
    val systemColumns = ctx(ctxKeyForSystem).columns.filter(_.name.startsWith(":"))
    systemStar.toSeq.flatMap(processStar(_, systemColumns)) ++
      userStars.flatMap { userStar =>
        val ctxKeyForUser = userStar.qualifier.getOrElse(defaultQualifier)
        val userColumns = ctx(ctxKeyForUser).columns.filterNot(_.name.startsWith(":"))
        processStar(userStar, userColumns)
      } ++
      expressions
  }

  /**
   * Expands a single [[com.socrata.soql.ast.StarSelection]] into a list of
   * [[com.socrata.soql.ast.SelectedExpression]]s.
   *
   * @param starSelection The star-selection to expand
   * @param columns The set of column-names into which this star would expand if
   *   there were no exceptions.
   *
   * @return A list of [[com.socrata.soql.ast.SelectedExpression]]s equivalent to
   *         this star, in the same order as they appear in `columns`
   *
   * @throws com.socrata.soql.exceptions.RepeatedException if the same column is EXCEPTed more than once
   * @throws com.socrata.soql.exceptions.NoSuchColumn if a column not on the dataset is excepted.
   */
  def processStar(starSelection: StarSelection, columns: OrderedSet[ColumnName])(implicit ctx: AnalysisContext): Seq[SelectedExpression] = {
    // TODO: Actually handle qualifier
    val StarSelection(qual, exceptions) = starSelection
    val exceptedColumnNames = new mutable.HashSet[ColumnName]
    for((column, position) <- exceptions) {
      if(exceptedColumnNames.contains(column)) throw RepeatedException(column, position)
      if(!columns.contains(column)) throw new NoSuchColumn.RealNoSuchColumn(qual, column, position)
      exceptedColumnNames += column
    }
    for {
      col <- columns.toIndexedSeq
      if !exceptedColumnNames(col)
    } yield SelectedExpression(ColumnOrAliasRef(starSelection.qualifier, col)(starSelection.starPosition), None)
  }

  /**
   * Assigns aliases to selections which are a simple identifier without
   * one and accepts aliases for expressions that have one.
   *
   * @note This relies on any columns from :* and/or * appearing first
   * in the input sequence in order to ensure that the position of any
   * [[com.socrata.soql.exceptions.DuplicateAlias]] is correct.
   *
   * @return The set of column names which were semi-explicitly assigned aliases together with
   *         a new selection list in the same order but with semi-explicit aliases assigned.
   * @throws com.socrata.soql.exceptions.DuplicateAlias if a duplicate alias is detected
   */
  def assignExplicitAndSemiExplicit(selections: Seq[SelectedExpression])(implicit ctx: AnalysisContext): (Set[ColumnName], Seq[SelectedExpression]) = {
    val (_, semiExplicit, newSelected) = selections.foldLeft((Set.empty[ColumnName], Set.empty[ColumnName], Vector.empty[SelectedExpression])) { (results, selection) =>
      val (assigned, semiExplicit, mapped) = results

      def register(alias: ColumnName, position: Position, expr: Expression, isExplicit: Boolean) = {
        if(assigned.contains(alias)) throw DuplicateAlias(alias, position)
        val newSemiExplicit = if(isExplicit) semiExplicit else semiExplicit + alias
        (assigned + alias, newSemiExplicit, mapped :+ SelectedExpression(expr, Some((alias, position))))
      }

      selection match {
        case SelectedExpression(expr, Some((alias, position))) =>
          register(alias, position, expr, isExplicit = true)
        case SelectedExpression(expr: ColumnOrAliasRef, None) =>
          register(expr.column, expr.position, expr, isExplicit = false)
        case other =>
          (assigned, semiExplicit, mapped :+ other)
      }
    }
    (semiExplicit, newSelected)
  }

  /**
   * Assigns aliases to non-simple expressions.
   *
   * @param selections The selection-list, with semi-explicit and explicit aliases already assigned
   * @return The fully-aliased selections, in the same order as they arrived
   */
  def assignImplicit(selections: Seq[SelectedExpression], semiExplicit: Set[ColumnName])(implicit ctx: AnalysisContext): OrderedMap[ColumnName, ExpressionInfo] = {
    val assignedAliases: Set[ColumnName] = selections.iterator.collect {
      case SelectedExpression(_, Some((alias, _))) => alias
    }.to(Set)
    selections.foldLeft((assignedAliases, OrderedMap.empty[ColumnName, ExpressionInfo])) { (acc, selection) =>
      val (assignedSoFar, mapped) = acc
      selection match {
        case SelectedExpression(expr, Some((name, _))) => // already has an alias
          (assignedSoFar, mapped + (name -> ExpressionInfo(expr, if(semiExplicit(name)) AliasType.SemiExplicit else AliasType.Explicit)))
        case SelectedExpression(expr, None) =>
          assert(!expr.isInstanceOf[ColumnOrAliasRef], "found un-aliased pure identifier")
          val newName = implicitAlias(expr, assignedSoFar)
          (assignedSoFar + newName, mapped + (newName -> ExpressionInfo(expr, AliasType.Implicit)))
      }
    }._2
  }

  /** Computes an alias for an expression.  The resulting alias is guaranteed
   * not conflict with anything in `existingColumns` or the dataset context's
   * `columns`.
   *
   * @param selection The expression for which to generate an alias
   * @param existingAliases The set of already-present aliases
   * @return A new name for the expression */
  def implicitAlias(selection: Expression, existingAliases: Set[ColumnName])(implicit ctx: AnalysisContext): ColumnName = {

    def inUse(name: ColumnName) =
      existingAliases.contains(name) ||
      ctx.exists { case (_, dsCtx) =>
        dsCtx.columns.contains(name)
      }

    val base = selection.toSyntheticIdentifierBase
    val firstTry = ColumnName(base)

    if(inUse(firstTry)) {
      val prefix = if(base.endsWith("_") || base.endsWith("-")) base
                   else base + "_"
      Iterator.from(1).
        map { i => ColumnName(prefix + i) }.
        dropWhile(inUse).
        next()
    } else {
      firstTry
    }
  }

  private def topoSort(graph: Map[ColumnName, Set[ColumnOrAliasRef]]): Seq[ColumnName] = {
    val visited = new mutable.HashSet[ColumnName]
    val result = new mutable.ListBuffer[ColumnName]
    def visit(n: ColumnName, seen: Set[ColumnName]): Unit = {
      if(!visited(n)) {
        visited += n
        val newSeen = seen + n
        for(m <- graph.getOrElse(n, Set.empty)) {
          if(seen contains m.column) throw CircularAliasDefinition(m.column, m.position)
          visit(m.column, newSeen)
        }
        result += n
      }
    }
    for(k <- graph.keys) visit(k, Set(k))
    result.toList
  }

  /** Produces a list of the order in which aliased expressions should be typechecked or
    * evaluated, based on their references to other aliases.
    *
    * @return the aliases in evaluation order
    * @throws com.socrata.soql.exceptions.CircularAliasDefinition if an alias's expansion refers to itself,
    *                                                             even indirectly. */
  def orderAliasesForEvaluation(in: OrderedMap[ColumnName, ExpressionInfo], semiExplicit: Set[ColumnName])(implicit ctx: AnalysisContext): Seq[ColumnName] = {
    // We'll divide all the aliases up into two categories -- aliases which refer
    // to a column of the same name ("selfRefs") and everything else ("otherRefs").
    val (semiExplicitRefs, otherRefs) = in.partition {
      case (alias, _) if semiExplicit(alias) => true
      case _ => false
    }

    val graph = otherRefs.transform { case (targetAlias, ExpressionInfo(expr, _)) =>
      // semi-explicit refs are non-circular, but since they _look_ circular we want to exclude them.
      // We also want to exclude references to things that are not aliased at all (either they're columns
      // on the dataset or they're not -- either way it'll be handled at typechecking).
      expr.allColumnRefs.filterNot { cr =>
        semiExplicit.contains(cr.column) || !in.contains(cr.column) || targetAlias == cr.column
      }
    }

    semiExplicitRefs.keys.toSeq ++ topoSort(graph)
  }
}
