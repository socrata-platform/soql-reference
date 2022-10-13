package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.Pretty

import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{FunctionInfo, HasType, HasDoc}
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

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
object Expr {
  implicit def serialize[CT: Writable, CV: Writable]: Writable[Expr[CT, CV]] = new Writable[Expr[CT, CV]] {
    def writeTo(buffer: WriteBuffer, t: Expr[CT, CV]): Unit =
      t match {
        case c : Column[CT] =>
          buffer.write(0)
          Column.serialize[CT].writeTo(buffer, c)
        case slr: SelectListReference[CT] =>
          buffer.write(1)
          SelectListReference.serialize[CT].writeTo(buffer, slr)
        case lv: LiteralValue[CT, CV] =>
          buffer.write(2)
          LiteralValue.serialize[CT, CV].writeTo(buffer, lv)
        case nl: NullLiteral[CT] =>
          buffer.write(3)
          NullLiteral.serialize[CT].writeTo(buffer, nl)
        case fc: FunctionCall[CT, CV] =>
          buffer.write(4)
          FunctionCall.serialize[CT, CV].writeTo(buffer, fc)
        case afc: AggregateFunctionCall[CT, CV] =>
          buffer.write(5)
          AggregateFunctionCall.serialize[CT, CV].writeTo(buffer, afc)
        case wfc: WindowedFunctionCall[CT, CV] =>
          buffer.write(6)
          WindowedFunctionCall.serialize[CT, CV].writeTo(buffer, wfc)
      }
  }

  implicit def deserialize[CT: Readable, CV: Readable](implicit hasType: HasType[CV, CT], functionInfo: FunctionInfo[CT]): Readable[Expr[CT, CV]] = new Readable[Expr[CT, CV]] {
    def readFrom(buffer: ReadBuffer): Expr[CT, CV] =
      buffer.read[Int]() match {
        case 0 => Column.deserialize[CT].readFrom(buffer)
        case 1 => SelectListReference.deserialize[CT].readFrom(buffer)
        case 2 => LiteralValue.deserialize[CT, CV].readFrom(buffer)
        case 3 => NullLiteral.deserialize[CT].readFrom(buffer)
        case 4 => FunctionCall.deserialize[CT, CV].readFrom(buffer)
        case 5 => AggregateFunctionCall.deserialize[CT, CV].readFrom(buffer)
        case 6 => WindowedFunctionCall.deserialize[CT, CV].readFrom(buffer)
      }
  }
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

object Column {
  def serialize[CT : Writable] = new Writable[Column[CT]] {
    def writeTo(buffer: WriteBuffer, c: Column[CT]): Unit = {
      buffer.write(c.table)
      buffer.write(c.column)
      buffer.write(c.typ)
      buffer.write(c.position)
    }
  }

  def deserialize[CT : Readable] = new Readable[Column[CT]] {
    def readFrom(buffer: ReadBuffer): Column[CT] = {
      Column(
        table = buffer.read[TableLabel](),
        column = buffer.read[ColumnLabel](),
        typ = buffer.read[CT]()
      )(
        buffer.read[Position]()
      )
    }
  }
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

object SelectListReference {
  def serialize[CT : Writable] = new Writable[SelectListReference[CT]] {
    def writeTo(buffer: WriteBuffer, slr: SelectListReference[CT]): Unit = {
      buffer.write(slr.index)
      buffer.write(slr.isAggregated)
      buffer.write(slr.isWindowed)
      buffer.write(slr.typ)
      buffer.write(slr.position)
    }
  }

  def deserialize[CT : Readable] = new Readable[SelectListReference[CT]] {
    def readFrom(buffer: ReadBuffer): SelectListReference[CT] = {
      SelectListReference(
        index = buffer.read[Int](),
        isAggregated = buffer.read[Boolean](),
        isWindowed = buffer.read[Boolean](),
        typ = buffer.read[CT]()
      )(
        buffer.read[Position]()
      )
    }
  }
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

object LiteralValue {
  def serialize[CT, CV: Writable] = new Writable[LiteralValue[CT, CV]] {
    def writeTo(buffer: WriteBuffer, lv: LiteralValue[CT, CV]): Unit = {
      buffer.write(lv.value)
      buffer.write(lv.position)
    }
  }

  def deserialize[CT, CV: Readable](implicit ev: HasType[CV, CT]) = new Readable[LiteralValue[CT, CV]] {
    def readFrom(buffer: ReadBuffer): LiteralValue[CT, CV] = {
      LiteralValue(
        buffer.read[CV]()
      )(
        buffer.read[Position]()
      )
    }
  }
}

final case class NullLiteral[+CT](typ: CT)(val position: Position) extends Literal[CT, Nothing] {
  type Self[+CT, +CV] = NullLiteral[CT]

  val size = 1

  protected def doDebugDoc(implicit ev: HasDoc[Nothing]) = d"NULL"

  def doRelabel(state: RelabelState) = this
  def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def reposition(p: Position): Self[CT, Nothing] = copy()(position = p)
}
object NullLiteral {
  def serialize[CT: Writable] = new Writable[NullLiteral[CT]] {
    def writeTo(buffer: WriteBuffer, nl: NullLiteral[CT]): Unit = {
      buffer.write(nl.typ)
      buffer.write(nl.position)
    }
  }

  def deserialize[CT: Readable] = new Readable[NullLiteral[CT]] {
    def readFrom(buffer: ReadBuffer): NullLiteral[CT] = {
      NullLiteral(
        buffer.read[CT]()
      )(
        buffer.read[Position]()
      )
    }
  }
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
object FunctionCall {
  def serialize[CT: Writable, CV: Writable] = new Writable[FunctionCall[CT, CV]] {
    def writeTo(buffer: WriteBuffer, fc: FunctionCall[CT, CV]): Unit = {
      buffer.write(fc.function.function.identity)
      buffer.write(fc.function.bindings)
      buffer.write(fc.args)
      buffer.write(fc.position)
      buffer.write(fc.functionNamePosition)
    }
  }

  def deserialize[CT: Readable, CV: Readable](implicit ht: HasType[CV, CT], fi: FunctionInfo[CT]) = new Readable[FunctionCall[CT, CV]] {
    def readFrom(buffer: ReadBuffer): FunctionCall[CT, CV] = {
      val function = fi.functionsByIdentity(buffer.read[String]())
      val bindings = buffer.read[Map[String, CT]]()
      val args = buffer.read[Seq[Expr[CT, CV]]]()
      val position = buffer.read[Position]()
      val functionNamePosition = buffer.read[Position]()
      FunctionCall(
        function = MonomorphicFunction(function, bindings),
        args = args
      )(
        position,
        functionNamePosition
      )
    }
  }
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
object AggregateFunctionCall {
  def serialize[CT: Writable, CV: Writable] = new Writable[AggregateFunctionCall[CT, CV]] {
    def writeTo(buffer: WriteBuffer, afc: AggregateFunctionCall[CT, CV]): Unit = {
      buffer.write(afc.function.function.identity)
      buffer.write(afc.function.bindings)
      buffer.write(afc.args)
      buffer.write(afc.distinct)
      buffer.write(afc.filter)
      buffer.write(afc.position)
      buffer.write(afc.functionNamePosition)
    }
  }

  def deserialize[CT: Readable, CV: Readable](implicit ht: HasType[CV, CT], fi: FunctionInfo[CT]) = new Readable[AggregateFunctionCall[CT, CV]] {
    def readFrom(buffer: ReadBuffer): AggregateFunctionCall[CT, CV] = {
      val function = fi.functionsByIdentity(buffer.read[String]())
      val bindings = buffer.read[Map[String, CT]]()
      val args = buffer.read[Seq[Expr[CT, CV]]]()
      val distinct = buffer.read[Boolean]()
      val filter = buffer.read[Option[Expr[CT, CV]]]()
      val position = buffer.read[Position]()
      val functionNamePosition = buffer.read[Position]()
      AggregateFunctionCall(
        MonomorphicFunction(function, bindings),
        args,
        distinct,
        filter
      )(
        position,
        functionNamePosition
      )
    }
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

object WindowedFunctionCall {
  def serialize[CT: Writable, CV: Writable] = new Writable[WindowedFunctionCall[CT, CV]] {
    def writeTo(buffer: WriteBuffer, wfc: WindowedFunctionCall[CT, CV]): Unit = {
      buffer.write(wfc.function.function.identity)
      buffer.write(wfc.function.bindings)
      buffer.write(wfc.args)
      buffer.write(wfc.filter)
      buffer.write(wfc.partitionBy)
      buffer.write(wfc.orderBy)
      buffer.write(wfc.frame)(Writable.option(Frame.serialize))
      buffer.write(wfc.position)
      buffer.write(wfc.functionNamePosition)
    }
  }

  def deserialize[CT: Readable, CV: Readable](implicit ht: HasType[CV, CT], fi: FunctionInfo[CT]) = new Readable[WindowedFunctionCall[CT, CV]] {
    def readFrom(buffer: ReadBuffer): WindowedFunctionCall[CT, CV] = {
      val function = fi.functionsByIdentity(buffer.read[String]())
      val bindings = buffer.read[Map[String, CT]]()
      val args = buffer.read[Seq[Expr[CT, CV]]]()
      val filter = buffer.read[Option[Expr[CT, CV]]]()
      val partitionBy = buffer.read[Seq[Expr[CT, CV]]]()
      val orderBy = buffer.read[Seq[OrderBy[CT, CV]]]()
      val frame = buffer.read[Option[Frame]]()(Readable.option(Frame.serialize))
      val position = buffer.read[Position]()
      val functionNamePosition = buffer.read[Position]()
      WindowedFunctionCall(
        MonomorphicFunction(function, bindings),
        args,
        filter,
        partitionBy,
        orderBy,
        frame
      )(
        position,
        functionNamePosition
      )
    }
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

object Frame {
  implicit object serialize extends Writable[Frame] with Readable[Frame] {
    def writeTo(buffer: WriteBuffer, t: Frame): Unit = {
      buffer.write(t.context)(FrameContext.serialize)
      buffer.write(t.start)(FrameBound.serialize)
      buffer.write(t.end)
      buffer.write(t.exclusion)(Writable.option(FrameExclusion.serialize))
    }

    def readFrom(buffer: ReadBuffer): Frame = {
      Frame(
        context = buffer.read[FrameContext]()(FrameContext.serialize),
        start = buffer.read[FrameBound]()(FrameBound.serialize),
        end = buffer.read[Option[FrameBound]](),
        exclusion = buffer.read[Option[FrameExclusion]]()(Readable.option(FrameExclusion.serialize))
      )
    }
  }
}

sealed abstract class FrameContext(val debugDoc: Doc[Nothing])
object FrameContext {
  case object Range extends FrameContext(d"RANGE")
  case object Rows extends FrameContext(d"ROWS")
  case object Groups extends FrameContext(d"GROUPS")

  implicit object serialize extends Writable[FrameContext] with Readable[FrameContext] {
    def writeTo(buffer: WriteBuffer, t: FrameContext): Unit = {
      val tag = t match {
        case Range => 0
        case Rows => 1
        case Groups => 2
      }
      buffer.write(tag)
    }

  def readFrom(buffer: ReadBuffer): FrameContext = {
      buffer.read[Int]() match {
        case 0 => Range
        case 1 => Rows
        case 2 => Groups
        case other => fail("Unknown frame context " + other)
      }
    }
  }
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

  implicit object serialize extends Writable[FrameBound] with Readable[FrameBound] {
    def writeTo(buffer: WriteBuffer, t: FrameBound): Unit = {
      t match {
        case UnboundedPreceding =>
          buffer.write(0)
        case Preceding(n) =>
          buffer.write(1)
          buffer.write(n)
        case CurrentRow =>
          buffer.write(2)
        case Following(n) =>
          buffer.write(3)
          buffer.write(n)
        case UnboundedFollowing =>
          buffer.write(4)
      }
    }

    def readFrom(buffer: ReadBuffer): FrameBound = {
      buffer.read[Int]() match {
        case 0 => UnboundedPreceding
        case 1 => Preceding(buffer.read[Long]())
        case 2 => CurrentRow
        case 3 => Following(buffer.read[Long]())
        case 4 => UnboundedFollowing
        case other => fail("Unknown frame bound tag: " + other)
      }
    }
  }
}

sealed abstract class FrameExclusion(val debugDoc: Doc[Nothing])
object FrameExclusion {
  case object CurrentRow extends FrameExclusion(d"CURRENT ROW")
  case object Group extends FrameExclusion(d"GROUP")
  case object Ties extends FrameExclusion(d"TIES")
  case object NoOthers extends FrameExclusion(d"NO OTHERS")

  implicit object serialize extends Writable[FrameExclusion] with Readable[FrameExclusion] {
    def writeTo(buffer: WriteBuffer, t: FrameExclusion): Unit = {
      val tag = t match {
        case CurrentRow => 0
        case Group => 1
        case Ties => 2
        case NoOthers => 3
      }
      buffer.write(tag)
    }

    def readFrom(buffer: ReadBuffer): FrameExclusion = {
      buffer.read[Int]() match {
        case 0 => CurrentRow
        case 1 => Group
        case 2 => Ties
        case 3 => NoOthers
        case other => fail("Unknown frame exclusion tag: " + other)
      }
    }
  }
}
