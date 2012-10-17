package com.socrata.soql.analysis

import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.mutable.HashSet

import com.socrata.soql.ast._
import com.socrata.soql.DatasetContext
import com.socrata.soql.names._
import collection.mutable

class RepeatedExceptionException(val name: ColumnName, val position: Position) extends Exception("Column `" + name + "' has already been excluded:\n" + position.longString)
class DuplicateAliasException(val name: ColumnName, val position: Position) extends Exception("There is already a column named `" + name + "' selected:\n" + position.longString)
class NoSuchColumnException(val name: ColumnName, val position: Position) extends Exception("No such column `" + name + "':\n" + position.longString)
class CircularAliasDefinitionException(val name: ColumnName, val position: Position) extends Exception("Circular reference while defining alias " + name + ":\n" + position.longString)

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
  def apply(selection: Selection)(implicit ctx: DatasetContext): Map[ColumnName, Expression] = {
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
   * @param starSelection The star-selection to expand
   * @param columns The set of column-names into which this star would expand if
   *   there were no exceptions.
   *
   * @throws RepeatedExceptionException if the same column is EXCEPTed more than once
   * @throws NoSuchColumnException if a column not on the dataset is excepted.
   */
  def processStar(starSelection: StarSelection, columns: Set[ColumnName])(implicit ctx: DatasetContext): Seq[SelectedExpression] = {
    val StarSelection(exceptions) = starSelection
    val exceptedColumnNames = new mutable.HashSet[ColumnName]
    for((column, position) <- exceptions) {
      if(exceptedColumnNames.contains(column)) throw new RepeatedExceptionException(column, position)
      if(!columns.contains(column)) throw new NoSuchColumnException(column, position)
      exceptedColumnNames += column
    }
    for {
      col <- columns.toIndexedSeq.sorted
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
   * @return A map from alias to expression, together with a sequence of
   *    non-simple expressions without aliases.
   * @throws DuplicateAliasException if the duplicate aliases are detected
   */
  def assignExplicitAndSemiExplicit(selections: Seq[SelectedExpression])(implicit ctx: DatasetContext): (Map[ColumnName, Expression], Seq[Expression]) = {
    selections.foldLeft((Map.empty[ColumnName, Expression], Vector.empty[Expression])) { (results, selection) =>
      val (assigned, unassigned) = results

      def register(ident: (ColumnName, Position), expr: Expression) = {
        val (name, position) = ident
        if(assigned.contains(name)) throw new DuplicateAliasException(name, position)
        (assigned + (name -> expr), unassigned)
      }

      selection match {
        case SelectedExpression(expr, Some(ident)) =>
          register(ident, expr)
        case SelectedExpression(expr: ColumnOrAliasRef, None) =>
          register((expr.column, expr.position), expr)
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
      assert(!selection.isInstanceOf[ColumnOrAliasRef], "found un-aliased pure identifier")
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

  private def topoSort(graph: Map[ColumnName, Set[ColumnOrAliasRef]]): Either[ColumnOrAliasRef, Seq[ColumnName]] = {
    class CircleFound(val node: ColumnOrAliasRef) extends Exception with scala.util.control.ControlThrowable
    try {
      val visited = new mutable.HashSet[ColumnName]
      val result = new mutable.ListBuffer[ColumnName]
      def visit(n: ColumnName, seen: Set[ColumnName]) {
        if(!visited(n)) {
          visited += n
          val newSeen = seen + n
          for(m <- graph.getOrElse(n, Set.empty)) {
            if(seen contains n) throw new CircleFound(m)
            visit(m.column, newSeen)
          }
          result += n
        }
      }
      for(k <- graph.keys) visit(k, Set.empty)
      Right(result.toList)
    } catch {
      case e: CircleFound => Left(e.node)
    }
  }

  def orderAliases(in: Map[ColumnName, Expression])(implicit ctx: DatasetContext): Seq[ColumnName] = {
    val (selfRefs, otherRefs) = in.partition {
      case (aliasName, ColumnOrAliasRef(colOrAliasName)) if aliasName == colOrAliasName => true
      case _ => false
    }

    val graph = otherRefs.mapValues { expr =>
      // The only column references which are interesting are those which point at
      // non-trivial aliases.
      expr.allColumnRefs.filter { cr =>
        otherRefs.contains(cr.column)
      }
    }

    topoSort(graph) match {
      case Right(order) =>
        selfRefs.keys.toSeq ++ order
      case Left(badNode) =>
        throw new CircularAliasDefinitionException(badNode.column, badNode.position)
    }
  }
}

object AA {
  import AliasAnalysis._
  def main(args: Array[String]) {
    implicit val datasetCtx = new DatasetContext {
      implicit val ctx = this
      val locale = com.ibm.icu.util.ULocale.ENGLISH
      val columns = Set(":id", ":created_at", "a", "b", "c", "d").map(ColumnName(_))
    }
    val p = new com.socrata.soql.parsing.Parser
    p.selection(":*, * (except b,c), a + c as b, d as c") match {
      case p.Success(sel, _) =>
        val aliased = apply(sel)
        println(aliased)
        val ordered = orderAliases(aliased)
        println(ordered)
    }
  }
}
