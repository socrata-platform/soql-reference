package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.Pretty

import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{HasType, HasDoc}

import DocUtils._

sealed abstract class Expr[+CT, +CV] extends Product {
  type Self[+CT, +CV] <: Expr[CT, CV]

  val typ: CT
  val position: Position

  val size: Int

  def isAggregated: Boolean
  def isWindowed: Boolean

  private[analyzer2] def doRewriteDatabaseNames(expr: RewriteDatabaseNamesState): Self[CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Self[CT, CV]

  private[analyzer2] def reposition(p: Position): Self[CT, CV]

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean
  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]]

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder = debugDoc.layoutSmart().toStringBuilder(sb)
  final def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[Nothing, CT]] =
    doDebugDoc.annotate(Annotation.Typed(typ))
  protected def doDebugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[Nothing, CT]]

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]]

  final def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean = find(_ == e).isDefined

  // Since this is completely immutable, cache the hashCode rather
  // than recomputing, as these trees can be quite deep.  Annoying
  // that it has to be lazy, but otherwise this gets initialized too
  // early.
  override final lazy val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)
}
final case class Column[+CT](table: TableLabel, column: ColumnLabel, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  type Self[+CT, +CV] = Column[CT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map(table -> Set(column))

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean = {
    that match {
      case Column(thatTable, thatColumn, thatTyp) =>
        this.typ == that.typ &&
          state.tryAssociate(Some(this.table), this.column, Some(thatTable), thatColumn)
      case _ =>
        false
    }
  }

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

  def doDebugDoc(implicit ev: HasDoc[Nothing]) =
    (table.debugDoc ++ d"." ++ column.debugDoc).
      annotate(Annotation.ColumnRef(table, column))

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)

  def find(predicate: Expr[CT, Nothing] => Boolean): Option[Expr[CT, Nothing]] = Some(this).filter(predicate)
}

final case class SelectListReference[+CT](index: Int, isAggregated: Boolean, isWindowed: Boolean, typ: CT)(val position: Position) extends Expr[CT, Nothing] {
  type Self[+CT, +CV] = SelectListReference[CT]

  val size = 1

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    throw new Exception("Cannot ask for ColumnReferences on a query with SelectListReferences")

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this

  private[analyzer2] def doRelabel(state: RelabelState) =
    this

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean = {
    this == that
  }

  protected def doDebugDoc(implicit ev: HasDoc[Nothing]) =
    Doc(index).annotate(Annotation.SelectListReference(index))

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)

  def find(predicate: Expr[CT, Nothing] => Boolean): Option[Expr[CT, Nothing]] = Some(this).filter(predicate)
}

sealed abstract class Literal[+CT, +CV] extends Expr[CT, CV] {
  type Self[+CT, +CV] <: Literal[CT, CV]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map.empty

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean = {
    this == that
  }

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] = Some(this).filter(predicate)
}
final case class LiteralValue[+CT, +CV](value: CV)(val position: Position)(implicit ev: HasType[CV, CT]) extends Literal[CT, CV] {
  type Self[+CT, +CV] = LiteralValue[CT, CV]

  val typ = ev.typeOf(value)
  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[CV]) = ev.docOf(value)

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def reposition(p: Position): Self[CT, CV] = copy()(position = p)
}
final case class NullLiteral[+CT](typ: CT)(val position: Position) extends Literal[CT, Nothing] {
  type Self[+CT, +CV] = NullLiteral[CT]

  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[Nothing]) = d"NULL"

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)
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
  def isWindowed = args.exists(_.isWindowed)

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    args.foldLeft(Map.empty[TableLabel, Set[ColumnLabel]]) { (acc, arg) =>
      acc.mergeWith(arg.columnReferences)(_ ++ _)
    }

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    Some(this).filter(predicate).orElse {
      args.iterator.flatMap(_.find(predicate)).nextOption()
    }

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean =
    that match {
      case FunctionCall(thatFunction, thatArgs) =>
        this.function == thatFunction &&
          this.args.length == thatArgs.length &&
          this.args.zip(thatArgs).forall { case (a, b) => a.findIsomorphism(state, b) }
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(args = args.map(_.doRewriteDatabaseNames(state)))(position, functionNamePosition)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)))(position, functionNamePosition)

  protected def doDebugDoc(implicit ev: HasDoc[CV]) =
    args.map(_.debugDoc).encloseHanging(Doc(function.name.name) ++ d"(", d",", d")")

  private[analyzer2] def reposition(p: Position): Self[CT, CV] = copy()(position = p, functionNamePosition)
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
  def isWindowed = args.exists(_.isWindowed)

  val size = 1 + args.iterator.map(_.size).sum + filter.fold(0)(_.size)

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = {
    var refs =
      args.foldLeft(Map.empty[TableLabel, Set[ColumnLabel]]) { (acc, arg) =>
        acc.mergeWith(arg.columnReferences)(_ ++ _)
      }
    for(f <- filter) {
      refs = refs.mergeWith(f.columnReferences)(_ ++ _)
    }
    refs
  }

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    Some(this).filter(predicate).orElse {
      args.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      filter.flatMap(_.find(predicate))
    }

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean =
    that match {
      case AggregateFunctionCall(thatFunction, thatArgs, thatDistinct, thatFilter) =>
        this.function == thatFunction &&
          this.args.length == thatArgs.length &&
          this.distinct == thatDistinct &&
          this.filter.isDefined == thatFilter.isDefined &&
          this.filter.zip(thatFilter).forall { case (a, b) => a.findIsomorphism(state, b) }
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    this.copy(
      args = args.map(_.doRewriteDatabaseNames(state)),
      filter = filter.map(_.doRewriteDatabaseNames(state))
    )(position, functionNamePosition)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)),
         filter = filter.map(_.doRelabel(state)))(position, functionNamePosition)

  protected def doDebugDoc(implicit ev: HasDoc[CV]) = {
    val preArgs = Seq(
      Some(Doc(function.name.name)),
      Some(d"("),
      if(distinct) Some(d"DISTINCT ") else None
    ).flatten.hcat
    val postArgs = Seq(
      Some(d")"),
      filter.map { w => w.debugDoc.encloseNesting(d"FILTER (", d")") }
    ).flatten.hsep
    args.map(_.debugDoc).encloseNesting(preArgs, d",", postArgs)
  }

  private[analyzer2] def reposition(p: Position): Self[CT, CV] = copy()(position = p, functionNamePosition)
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
  def isWindowed = true

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = {
    var refs =
      args.foldLeft(Map.empty[TableLabel, Set[ColumnLabel]]) { (acc, arg) =>
        acc.mergeWith(arg.columnReferences)(_ ++ _)
      }
    for(f <- filter) {
      refs = refs.mergeWith(f.columnReferences)(_ ++ _)
    }
    for(pb <- partitionBy) {
      refs = refs.mergeWith(pb.columnReferences)(_ ++ _)
    }
    for(ob <- orderBy) {
      refs = refs.mergeWith(ob.expr.columnReferences)(_ ++ _)
    }
    refs
  }

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    Some(this).filter(predicate).orElse {
      args.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      filter.flatMap(_.find(predicate))
    }.orElse {
      partitionBy.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      orderBy.iterator.flatMap(_.expr.find(predicate)).nextOption()
    }

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean =
    that match {
      case WindowedFunctionCall(thatFunction, thatArgs, thatFilter, thatPartitionBy, thatOrderBy, thatFrame) =>
        this.function == thatFunction &&
          this.args.length == thatArgs.length &&
          this.args.zip(thatArgs).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.filter.isDefined == thatFilter.isDefined &&
          this.filter.zip(thatFilter).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.partitionBy.length == thatPartitionBy.length &&
          this.partitionBy.zip(thatPartitionBy).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.orderBy.length == thatOrderBy.length &&
          this.orderBy.zip(thatOrderBy).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.frame == thatFrame
      case _ =>
        false
    }

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

  protected def doDebugDoc(implicit ev: HasDoc[CV]) = {
    val preArgs: Doc[Nothing] = Doc(function.name.name) ++ d"("
    val windowParts: Doc[Annotation[Nothing, CT]] =
      Seq[Option[Doc[Annotation[Nothing, CT]]]](
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
      filter.map { w => w.debugDoc.encloseNesting(d"FILTER (", d")") },
      Some(d"OVER" +#+ windowParts)
    ).flatten.hsep
    args.map(_.debugDoc).encloseNesting(preArgs, d",", postArgs)
  }

  private[analyzer2] def reposition(p: Position): Self[CT, CV] = copy()(position = p, functionNamePosition)
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
