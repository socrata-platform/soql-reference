package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasType

sealed abstract class Expr[+CT, +CV] {
  val typ: CT
  val position: Position

  val size: Int

  def isAggregated: Boolean

  private[analyzer2] def doRewriteDatabaseNames(expr: RewriteDatabaseNamesState): Expr[CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Expr[CT, CV]

  final def debugStr: String = debugStr(new StringBuilder).toString
  def debugStr(sb: StringBuilder): StringBuilder
}
case class Column[+CT](table: TableLabel, column: ColumnLabel, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
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

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append(table).append('.').append(column)
}

case class SelectListReference[+CT](index: Int, isAggregated: Boolean, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  val size = 1

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append(index)
}

sealed abstract class Literal[+CT, +CV] extends Expr[CT, CV] {
  def isAggregated = false

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this

  private[analyzer2] def doRelabel(state: RelabelState) = this
}
case class LiteralValue[+CT, +CV](value: CV)(val position: Position)(implicit ev: HasType[CV, CT]) extends Literal[CT, CV] {
  val typ = ev.typeOf(value)
  val size = 1

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append(value)
}
case class NullLiteral[+CT](typ: CT)(val position: Position) extends Literal[CT, Nothing] {
  val size = 1

  override def debugStr(sb: StringBuilder): StringBuilder =
    sb.append("NULL")
}

sealed trait FuncallLike[+CT, +CV] extends Expr[CT, CV] with Product {
  val function: MonomorphicFunction[CT]
  val functionNamePosition: Position

  override final def equals(that: Any): Boolean =
    that match {
      case null => false
      case thing: AnyRef if thing eq this => true // short circuit identity
      case thing if thing.getClass == this.getClass =>
        this.productIterator.zip(thing.asInstanceOf[Product].productIterator).forall { case (a, b) => a == b }
      case _ => false
    }
}

case class FunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  require(!function.isAggregate)
  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum
  def isAggregated = args.exists(_.isAggregated)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(args = args.map(_.doRewriteDatabaseNames(state)))(position, functionNamePosition)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)))(position, functionNamePosition)

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(function.name).append('(')
    var didOne = false
    for(arg <- args) {
      if(didOne) sb.append(", ")
      else didOne = true
      arg.debugStr(sb)
    }
    sb.append(')')
  }
}
case class AggregateFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  distinct: Boolean,
  filter: Option[Expr[CT, CV]]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
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

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(function.name).append('(')
    if(distinct) {
      sb.append("DISTINCT ")
    }
    var didOne = false
    for(arg <- args) {
      if(didOne) sb.append(", ")
      else didOne = true
      arg.debugStr(sb)
    }
    sb.append(')')
    for(f <- filter) {
      sb.append(" FILTER (WHERE ")
      f.debugStr(sb)
      sb.append(')')
    }
    sb
  }
}
case class WindowedFunctionCall[+CT, +CV](
  function: MonomorphicFunction[CT],
  args: Seq[Expr[CT, CV]],
  partitionBy: Seq[Expr[CT, CV]], // is normal right here, or should it be aggregate?
  orderBy: Seq[OrderBy[CT, CV]], // ditto thus
  context: FrameContext,
  start: FrameBound,
  end: Option[FrameBound],
  exclusion: Option[FrameExclusion]
)(val position: Position, val functionNamePosition: Position) extends Expr[CT, CV] {
  require(function.needsWindow)

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

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append("[WINDOW FUNCTION STUFF]")
  }
}

sealed abstract class FrameContext
object FrameContext {
  case object Range extends FrameContext
  case object Rows extends FrameContext
  case object Groups extends FrameContext
}

sealed abstract class FrameBound
object FrameBound {
  case object UnboundedPreceding extends FrameBound
  case class Preceding(n: Long) extends FrameBound
  case object CurrentRow extends FrameBound
  case class Following(n: Long) extends FrameBound
  case object UnboundedFollowing extends FrameBound
}

sealed abstract class FrameExclusion
object FrameExclusion {
  case object CurrentRow extends FrameExclusion
  case object Group extends FrameExclusion
  case object Ties extends FrameExclusion
  case object NoOthers extends FrameExclusion
}
