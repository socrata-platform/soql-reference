package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.Pretty

import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{HasType, HasDoc}

import DocUtils._

sealed abstract class Expr[+CT, +CV] extends Product {
  type Self[+CT, +CV] <: Expr[CT, CV]

  val typ: CT
  val position: Position

  val size: Int

  def isAggregated: Boolean

  private[analyzer2] def doRewriteDatabaseNames(expr: RewriteDatabaseNamesState): Self[CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Self[CT, CV]

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder = debugDoc.layoutSmart().toStringBuilder(sb)
  def debugDoc(implicit ev: HasDoc[CV]): Doc[ResourceAnn[Nothing, CT]]

  // Since this is completely immutable, cache the hashCode rather
  // than recomputing, as these trees can be quite deep.  Annoying
  // that it has to be lazy, but otherwise this gets initialized too
  // early.
  override final lazy val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)
}
final case class Column[+CT](table: TableLabel, column: ColumnLabel, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  type Self[+CT, +CV] = Column[CT]

  def isAggregated = false

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    column match {
      case dcn: DatabaseColumnName =>
        copy(column = state.convert(table, dcn))(position)
      case _ =>
        this
    }

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(table = state.convert(table), column = state.convert(column))(position)

  val size = 1

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).annotate(ResourceAnn.from(table, column)).annotate(ResourceAnn.from(typ))
}

final case class SelectListReference[+CT](index: Int, isAggregated: Boolean, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  type Self[+CT, +CV] = SelectListReference[CT]

  val size = 1

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  def debugDoc(implicit ev: HasDoc[Nothing]) = Doc(index.toString).annotate(ResourceAnn.from(typ))
}

sealed abstract class Literal[+CT, +CV] extends Expr[CT, CV] {
  type Self[+CT, +CV] <: Literal[CT, CV]

  def isAggregated = false
}
final case class LiteralValue[+CT, +CV](value: CV)(val position: Position)(implicit ev: HasType[CV, CT]) extends Literal[CT, CV] {
  type Self[+CT, +CV] = LiteralValue[CT, CV]

  val typ = ev.typeOf(value)
  val size = 1

  def debugDoc(implicit ev: HasDoc[CV]) =
    ev.docOf(value).annotate(ResourceAnn.from(typ))

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
}
final case class NullLiteral[+CT](typ: CT)(val position: Position) extends Literal[CT, Nothing] {
  type Self[+CT, +CV] = NullLiteral[CT]

  val size = 1

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    d"NULL".annotate(ResourceAnn.from(typ))

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
}

sealed trait FuncallLike[+CT, +CV] extends Expr[CT, CV] with Product {
  type Self[+CT, +CV] <: FuncallLike[CT, CV]

  val function: MonomorphicFunction[CT]
  val functionNamePosition: Position
}

final case class FunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends FuncallLike[CT, CV] {
  type Self[+CT, +CV] = FunctionCall[CT, CV]

  require(!function.isAggregate)
  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum
  def isAggregated = args.exists(_.isAggregated)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(args = args.map(_.doRewriteDatabaseNames(state)))(position, functionNamePosition)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)))(position, functionNamePosition)

  def debugDoc(implicit ev: HasDoc[CV]) =
    args.map(_.debugDoc).encloseHanging(Doc(function.name.name) ++ d"(", d",", d")").annotate(ResourceAnn.from(typ))
}
final case class AggregateFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  distinct: Boolean,
  filter: Option[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends FuncallLike[CT, CV] {
  type Self[+CT, +CV] = AggregateFunctionCall[CT, CV]

  require(function.isAggregate)
  val typ = function.result
  def isAggregated = true

  val size = 1 + args.iterator.map(_.size).sum + filter.fold(0)(_.size)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(state)),
      filter = filter.map(_.doRewriteDatabaseNames(state))
    )(position, functionNamePosition)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)),
         filter = filter.map(_.doRelabel(state)))(position, functionNamePosition)

  def debugDoc(implicit ev: HasDoc[CV]) = {
    val preArgs = Seq(
      Some(Doc(function.name.name)),
      Some(d"("),
      if(distinct) Some(d"DISTINCT ") else None
    ).flatten.hcat
    val postArgs = Seq(
      Some(d")"),
      filter.map { w => w.debugDoc.encloseNesting(d"FILTER (", d")") }
    ).flatten.hsep
    args.map(_.debugDoc).encloseNesting(preArgs, d",", postArgs).annotate(ResourceAnn.from(typ))
  }
}
final case class WindowedFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  filter: Option[Expr[CT, CV]],
  partitionBy: Seq[Expr[CT, CV]], // is normal right here, or should it be aggregate?
  orderBy: Seq[OrderBy[CT, CV]], // ditto thus
  frame: Option[Frame]
)(val position: Position, val functionNamePosition: Position) extends FuncallLike[CT, CV] {
  type Self[+CT, +CV] = WindowedFunctionCall[CT, CV]

  require(function.needsWindow || function.isAggregate)

  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum + partitionBy.iterator.map(_.size).sum + orderBy.iterator.map(_.expr.size).sum

  def isAggregated = args.exists(_.isAggregated) || partitionBy.exists(_.isAggregated) || orderBy.exists(_.expr.isAggregated)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(state)),
      partitionBy = partitionBy.map(_.doRewriteDatabaseNames(state)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(state))
    )(position, functionNamePosition)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)),
         partitionBy = partitionBy.map(_.doRelabel(state)),
         orderBy = orderBy.map(_.doRelabel(state)))(position, functionNamePosition)

  def debugDoc(implicit ev: HasDoc[CV]) = {
    val preArgs: Doc[Nothing] = Doc(function.name.name) ++ d"("
    val windowParts: Doc[ResourceAnn[Nothing, CT]] =
      Seq[Option[Doc[ResourceAnn[Nothing, CT]]]](
        if(partitionBy.nonEmpty) {
          Some((d"PARTITION BY" +: partitionBy.map(_.debugDoc).punctuate(d",")).sep.nest(2))
        } else {
          None
        },
        if(orderBy.nonEmpty) {
          Some((d"ORDER BY" +: orderBy.map(_.debugDoc).punctuate(d",")).sep.nest(2))
        } else {
          None
        },
        frame.map(_.debugDoc)
      ).flatten.sep.encloseNesting(d"(", d")")
    val postArgs = Seq(
      Some(d")"),
      filter.map { w => w.debugDoc.encloseNesting(d"FILTER (", d") OVER" +#+ windowParts) }
    ).flatten.hsep
    args.map(_.debugDoc).encloseNesting(preArgs, d",", postArgs).annotate(ResourceAnn.from(typ))
  }
}

case class Frame(
  context: FrameContext,
  start: FrameBound,
  end: Option[FrameBound],
  exclusion: Option[FrameExclusion]
) {
  def debugDoc =
    Seq(
      Some(context.debugDoc),
      Some(end match {
        case None => start.debugDoc
        case Some(end) =>
          Seq(d"BETWEEN", start.debugDoc, d"AND", end.debugDoc).hsep
      }),
      exclusion.map { e =>
        d"EXCLUDE" +#+ e.debugDoc
      }
    ).flatten.sep
}

sealed abstract class FrameContext(val debugDoc: Doc[Nothing])
object FrameContext {
  case object Range extends FrameContext(d"RANGE")
  case object Rows extends FrameContext(d"ROWS")
  case object Groups extends FrameContext(d"GROUPS")
}

sealed abstract class FrameBound {
  def debugDoc: Doc[Nothing]
}
object FrameBound {
  case object UnboundedPreceding extends FrameBound {
    def debugDoc = d"UNBOUNDED PRECEDING"
  }
  case class Preceding(n: Long) extends FrameBound {
    def debugDoc = d"$n PRECEDING"
  }
  case object CurrentRow extends FrameBound {
    def debugDoc = d"CURRENT ROW"
  }
  case class Following(n: Long) extends FrameBound {
    def debugDoc = d"$n FOLLOWING"
  }
  case object UnboundedFollowing extends FrameBound {
    def debugDoc = d"UNBOUNDED FOLLOWING"
  }
}

sealed abstract class FrameExclusion(val debugDoc: Doc[Nothing])
object FrameExclusion {
  case object CurrentRow extends FrameExclusion(d"CURRENT ROW")
  case object Group extends FrameExclusion(d"GROUP")
  case object Ties extends FrameExclusion(d"TIES")
  case object NoOthers extends FrameExclusion(d"NO OTHERS")
}
