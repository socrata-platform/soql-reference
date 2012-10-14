package com.socrata.soql.analysis

import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.mutable.HashSet

import com.socrata.soql.ast._
import com.socrata.soql.{ColumnName, DatasetContext}

class RepeatedExceptionException(val name: String, val position: Position) extends Exception("Column `" + name + "' has already been excluded:\n" + position.longString)
class DuplicateAliasException(val name: String, val position: Position) extends Exception("There is already a column named `" + name + "' selected:\n" + position.longString)
class NoSuchColumnException(val name: String, val position: Position) extends Exception("No such column `" + name + "':\n" + position.longString)

trait AliasAnalysis {
  def apply(selection: Selection)(implicit ctx: DatasetContext): Map[ColumnName, Expression]
}

object AliasAnalysis extends AliasAnalysis {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[AliasAnalysis])

  /** Validate and generate aliases for selected columns.
   *
   * @return a map from each alias to its corresponding expression
   *
   * @throws RepeatedExceptionException if the same column is excepted more than once
   * @throws NoSuchColumnException if a column not on the dataset is excepted
   * @throws DuplicateAliasException if the duplicate aliases are detected
   */
  def apply(selection: Selection)(implicit ctx: DatasetContext) = {
    log.debug("Input: {}", selection)
    val starsExpanded = expandSelection(selection)
    log.debug("After expanding stars: {}", starsExpanded)
    val (aliases, remaining) = assignExplicitAndSemiExplicit(starsExpanded)
    log.debug("Explicit and semi-explicit aliases: {}", aliases)
    log.debug("Remaining expressions to alias: {}", remaining)
    val allAliases = assignImplicit(aliases, remaining)
    log.debug("All aliases: {}", allAliases)
    allAliases
  }

  /**
   * Expands the `:*` and `*` in a selection-list into an explicit list of all the
   * selections in the provided [[com.socrata.soql.ast.Selection]].
   *
   * @param selection A selection-list to desugar.
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
   * @param ss The star-selection to expand
   * @param columns The set of column-names into which this star would expand if
   *   there were no exceptions.
   *
   * @throws RepeatedExceptionException if the same column is EXCEPTed more than once
   * @throws NoSuchColumnException if a column not on the dataset is excepted.
   */
  def processStar(ss: StarSelection, columns: Set[ColumnName])(implicit ctx: DatasetContext): Seq[SelectedExpression] = {
    val StarSelection(exceptions, position) = ss
    val exceptedColumnNames = new HashSet[ColumnName]
    for(exception <- exceptions) {
      val col = new ColumnName(exception.name)
      if(exceptedColumnNames.contains(col)) throw new RepeatedExceptionException(exception.name, exception.position)
      if(!columns.contains(col)) throw new NoSuchColumnException(exception.name, exception.position)
      exceptedColumnNames += col
    }
    for {
      col <- columns.toIndexedSeq.sorted
      if !exceptedColumnNames(col)
    } yield SelectedExpression(Identifier(col.name, false, position), None)
  }

  /**
   * Assigns aliases to selections which are a simple identifier without
   * one and accepts aliases for expressions that have one.
   *
   * @note This relies on any columns from :* and/or * appearing first
   * in the input sequence in order to ensure that the position of any
   * [[com.socrata.soql.analysis.DuplicateAliasException]] is correct.
   *
   * @return A map from alias to expression, together with a sequence of
   *    non-simple expressions without aliases.
   * @throws DuplicateAliasException if the duplicate aliases are detected
   */
  def assignExplicitAndSemiExplicit(selections: Seq[SelectedExpression])(implicit ctx: DatasetContext): (Map[ColumnName, Expression], Seq[Expression]) = {
    selections.foldLeft((Map.empty[ColumnName, Expression], Vector.empty[Expression])) { (results, selection) =>
      val (assigned, unassigned) = results

      def register(ident: Identifier, expr: Expression) = {
        val colName = new ColumnName(ident.name)
        if(assigned.contains(colName)) throw new DuplicateAliasException(ident.name, ident.position)
        (assigned + (colName -> expr), unassigned)
      }

      selection match {
        case SelectedExpression(expr, Some(ident)) =>
          register(ident, expr)
        case SelectedExpression(expr: Identifier, None) =>
          register(expr, expr)
        case SelectedExpression(expr, None) =>
          (assigned, unassigned :+ expr)
      }
    }
  }

  /**
   * Assigns aliases to non-simple expressions.
   *
   * @param assignedAliases Already-assigned aliases
   * @param unassignedSelections The list of expressions for which to generate aliases
   * @return A map from alias to expression for all the expressions, including the `assignedAliases`
   */
  def assignImplicit(assignedAliases: Map[ColumnName, Expression], unassignedSelections: Seq[Expression])(implicit ctx: DatasetContext): Map[ColumnName, Expression] = {
    unassignedSelections.foldLeft(assignedAliases) { (results, selection) =>
      assert(!selection.isInstanceOf[Identifier], "found un-aliased pure identifier")
      val newName = implicitAlias(selection, results.keySet)
      results + (newName -> selection)
    }
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
    val firstTry = new ColumnName(base)

    if(inUse(firstTry)) {
      val prefix = if(base.endsWith("_") || base.endsWith("-")) base
                   else base + "_"
      Iterator.from(1).
        map { i => new ColumnName(prefix + i) }.
        dropWhile(inUse).
        next()
    } else {
      firstTry
    }
  }
}
