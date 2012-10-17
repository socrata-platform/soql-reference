package com.socrata.soql.analysis

import util.parsing.input.Position

import com.socrata.soql.ast._
import com.socrata.soql.DatasetContext
import com.socrata.soql.names._
import collection.mutable
import com.socrata.collection.{OrderedMap, OrderedSet}

class RepeatedExceptionException(val name: ColumnName, val position: Position) extends Exception("Column `" + name + "' has already been excluded:\n" + position.longString)
class DuplicateAliasException(val name: ColumnName, val position: Position) extends Exception("There is already a column named `" + name + "' selected:\n" + position.longString)
class NoSuchColumnException(val name: ColumnName, val position: Position) extends Exception("No such column `" + name + "':\n" + position.longString)
class CircularAliasDefinitionException(val name: ColumnName, val position: Position) extends Exception("Circular reference while defining alias " + name + ":\n" + position.longString)

trait AliasAnalysis {
  case class Analysis(expressions: OrderedMap[ColumnName, Expression], evaluationOrder: Seq[ColumnName])
  def apply(selection: Selection)(implicit ctx: DatasetContext): Analysis
}

object AliasAnalysis extends AliasAnalysis {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[AliasAnalysis])

  /** Validate and generate aliases for selected columns.
    *
    * @return an Analysis object which contains an ordered map from each alias to its expansion
    *         and a list of the aliases in the order they should be typechecked/evaluated.
    *
    * @throws RepeatedExceptionException if the same column is excepted more than once
    * @throws NoSuchColumnException if a column not on the dataset is excepted
    * @throws DuplicateAliasException if duplicate aliases are detected
    * @throws CircularAliasDefinitionException if an alias is defined in terms of itself
    */
  def apply(selection: Selection)(implicit ctx: DatasetContext): Analysis = {
    log.debug("Input: {}", selection)
    val starsExpanded = expandSelection(selection)
    log.debug("After expanding stars: {}", starsExpanded)
    val explicitAndSemiExplicitAssigned = assignExplicitAndSemiExplicit(starsExpanded)
    log.debug("After assigning explicit and semi-explicit: {}", explicitAndSemiExplicitAssigned)
    val allAliasesAssigned = assignImplicit(explicitAndSemiExplicitAssigned)
    log.debug("After assigning implicit aliases: {}", allAliasesAssigned)
    Analysis(allAliasesAssigned, orderAliasesForEvaluation(allAliasesAssigned))
  }

  /**
   * Expands the `:*` and `*` in a selection-list into an explicit list of all the
   * selections in the provided [[com.socrata.soql.ast.Selection]].
   *
   * @param selection A selection-list to desugar.
   * @return The same list with any stars expanded
   *
   * @throws RepeatedExceptionException if the same column is excepted more than once
   * @throws NoSuchColumnException if a column not on the dataset is excepted
   */
  def expandSelection(selection: Selection)(implicit ctx: DatasetContext): Seq[SelectedExpression] = {
    val Selection(systemStar, userStar, expressions) = selection
    val (systemColumns, userColumns) = ctx.columns.partition(_.canonicalName.startsWith(":"))
    systemStar.toSeq.flatMap(processStar(_, systemColumns)) ++
      userStar.toSeq.flatMap(processStar(_, userColumns)) ++
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
   * @throws RepeatedExceptionException if the same column is EXCEPTed more than once
   * @throws NoSuchColumnException if a column not on the dataset is excepted.
   */
  def processStar(starSelection: StarSelection, columns: OrderedSet[ColumnName])(implicit ctx: DatasetContext): Seq[SelectedExpression] = {
    val StarSelection(exceptions) = starSelection
    val exceptedColumnNames = new mutable.HashSet[ColumnName]
    for((column, position) <- exceptions) {
      if(exceptedColumnNames.contains(column)) throw new RepeatedExceptionException(column, position)
      if(!columns.contains(column)) throw new NoSuchColumnException(column, position)
      exceptedColumnNames += column
    }
    for {
      col <- columns.toIndexedSeq
      if !exceptedColumnNames(col)
    } yield SelectedExpression(ColumnOrAliasRef(col).positionedAt(starSelection.starPosition), None)
  }

  /**
   * Assigns aliases to selections which are a simple identifier without
   * one and accepts aliases for expressions that have one.
   *
   * @note This relies on any columns from :* and/or * appearing first
   * in the input sequence in order to ensure that the position of any
   * [[com.socrata.soql.analysis.DuplicateAliasException]] is correct.
   *
   * @return A new selection list in the same order but with semi-explicit
   *         aliases assigned.
   * @throws DuplicateAliasException if a duplicate alias is detected
   */
  def assignExplicitAndSemiExplicit(selections: Seq[SelectedExpression])(implicit ctx: DatasetContext): Seq[SelectedExpression] = {
    selections.foldLeft((Set.empty[ColumnName], Vector.empty[SelectedExpression])) { (results, selection) =>
      val (assigned, mapped) = results

      def register(alias: ColumnName, position: Position, expr: Expression) = {
        if(assigned.contains(alias)) throw new DuplicateAliasException(alias, position)
        (assigned + alias, mapped :+ SelectedExpression(expr, Some((alias, position))))
      }

      selection match {
        case SelectedExpression(expr, Some((alias, position))) =>
          register(alias, position, expr)
        case SelectedExpression(expr: ColumnOrAliasRef, None) =>
          register(expr.column, expr.position, expr)
        case other =>
          (assigned, mapped :+ other)
      }
    }._2
  }

  /**
   * Assigns aliases to non-simple expressions.
   *
   * @param selections The selection-list, with semi-explicit and explicit aliases already assigned
   * @return The fully-aliased selections, in the same order as they arrived
   */
  def assignImplicit(selections: Seq[SelectedExpression])(implicit ctx: DatasetContext): OrderedMap[ColumnName, Expression] = {
    val assignedAliases: Set[ColumnName] = selections.collect {
      case SelectedExpression(_, Some((alias, _))) => alias
    } (scala.collection.breakOut)
    selections.foldLeft((assignedAliases, OrderedMap.empty[ColumnName, Expression])) { (acc, selection) =>
      val (assignedSoFar, mapped) = acc
      selection match {
        case SelectedExpression(expr, Some((name, _))) => // already has an alias
          (assignedSoFar, mapped + (name -> expr))
        case SelectedExpression(expr, None) =>
          assert(!expr.isInstanceOf[ColumnOrAliasRef], "found un-aliased pure identifier")
          val newName = implicitAlias(expr, assignedSoFar)
          (assignedSoFar + newName, mapped + (newName -> expr))
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
  def implicitAlias(selection: Expression, existingAliases: Set[ColumnName])(implicit ctx: DatasetContext): ColumnName = {
    def inUse(name: ColumnName) = existingAliases.contains(name) || ctx.columns.contains(name)

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
    def visit(n: ColumnName, seen: Set[ColumnName]) {
      if(!visited(n)) {
        visited += n
        val newSeen = seen + n
        for(m <- graph.getOrElse(n, Set.empty)) {
          if(seen contains n) throw new CircularAliasDefinitionException(m.column, m.position)
          visit(m.column, newSeen)
        }
        result += n
      }
    }
    for(k <- graph.keys) visit(k, Set.empty)
    result.toList
  }

  /** Produces a list of the order in which aliased expressions should be typechecked or
    * evaluated, based on their references to other aliases.
    *
    * @return the aliases in evaluation order
    * @throws CircularAliasDefinitionException if an alias's expansion refers to itself, even indirectly. */
  def orderAliasesForEvaluation(in: OrderedMap[ColumnName, Expression])(implicit ctx: DatasetContext): Seq[ColumnName] = {
    // We'll divide all the aliases up into two categories -- aliases which refer
    // to a column of the same name ("selfRefs") and everything else ("otherRefs").
    val (selfRefs, otherRefs) = in.partition {
      case (aliasName, ColumnOrAliasRef(colOrAliasName)) if aliasName == colOrAliasName => true
      case _ => false
    }

    val graph = otherRefs.mapValues { expr =>
      // selfRefs are non-circular, but since they _look_ circular we want to exclude them.
      // This also excludes references to columns which don't exist at all, but that'll be
      // caught at type-checking time.
      expr.allColumnRefs.filter { cr =>
        otherRefs.contains(cr.column)
      }
    }

    selfRefs.keys.toSeq ++ topoSort(graph)
  }
}

object AA {
  import AliasAnalysis._
  def main(args: Array[String]) {
    implicit val datasetCtx = new DatasetContext {
      implicit val ctx = this
      val locale = com.ibm.icu.util.ULocale.ENGLISH
      val columns = com.socrata.collection.OrderedSet(":id", ":created_at", "a", "b", "c", "d").map(ColumnName(_))
    }
    val p = new com.socrata.soql.parsing.Parser
    println(datasetCtx.columns)
    p.selection(":*, * (except b,c), a + c as b, d as c") match {
      case p.Success(sel, _) =>
        val aliased = apply(sel)
        println(aliased)
    }
  }
}
