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

private[analyzer2] trait HashedExpr { this: Product =>
  // Since this is completely immutable, cache the hashCode rather
  // than recomputing, as these trees can be quite deep.  This is a
  // mixin so it can be computed after the various case classes are
  // fully constructed.
  override final val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)
}

sealed abstract class Expr[MT <: MetaTypes] extends Product with MetaTypeHelper[MT] with LabelHelper[MT] { this: HashedExpr =>
  type Self[MT <: MetaTypes] <: Expr[MT]

  val typ: CT
  val position: PositionInfo

  val size: Int

  def isAggregated: Boolean
  def isWindowed: Boolean

  private[analyzer2] def doRewriteDatabaseNames(expr: RewriteDatabaseNamesState): Self[MT]

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT]

  private[analyzer2] def reposition(p: Position): Self[MT]

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean
  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]]

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder = debugDoc.layoutSmart().toStringBuilder(sb)
  final def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[MT]] =
    doDebugDoc.annotate(Annotation.Typed(typ))
  protected def doDebugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[MT]]

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]]

  final def contains(e: Expr[MT]): Boolean = find(_ == e).isDefined
}
object Expr {
  implicit def serialize[MT <: MetaTypes](implicit writableCT: Writable[MT#CT], writableCV: Writable[MT#CV], writableDTN: Writable[MT#DatabaseTableNameImpl], writableDCN: Writable[MT#DatabaseColumnNameImpl]): Writable[Expr[MT]] = new Writable[Expr[MT]] {
    implicit val self = this
    def writeTo(buffer: WriteBuffer, t: Expr[MT]): Unit =
      t match {
        case c : Column[MT] =>
          buffer.write(0)
          buffer.write(c)
        case slr: SelectListReference[MT] =>
          buffer.write(1)
          buffer.write(slr)
        case lv: LiteralValue[MT] =>
          buffer.write(2)
          buffer.write(lv)
        case nl: NullLiteral[MT] =>
          buffer.write(3)
          buffer.write(nl)
        case fc: FunctionCall[MT] =>
          buffer.write(4)
          buffer.write(fc)
        case afc: AggregateFunctionCall[MT] =>
          buffer.write(5)
          buffer.write(afc)
        case wfc: WindowedFunctionCall[MT] =>
          buffer.write(6)
          buffer.write(wfc)
      }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT: Readable[MT#CT], readableCV: Readable[MT#CV], hasType: HasType[MT#CV, MT#CT], mf: Readable[MonomorphicFunction[MT#CT]], readableDTN: Readable[MT#DatabaseTableNameImpl], readableDCN: Readable[MT#DatabaseColumnNameImpl]): Readable[Expr[MT]] = new Readable[Expr[MT]] {
    implicit val self = this
    def readFrom(buffer: ReadBuffer): Expr[MT] =
      buffer.read[Int]() match {
        case 0 => buffer.read[Column[MT]]()
        case 1 => buffer.read[SelectListReference[MT]]()
        case 2 => buffer.read[LiteralValue[MT]]()
        case 3 => buffer.read[NullLiteral[MT]]()
        case 4 => buffer.read[FunctionCall[MT]]()
        case 5 => buffer.read[AggregateFunctionCall[MT]]()
        case 6 => buffer.read[WindowedFunctionCall[MT]]()
        case other => fail("Unknown expression tag " + other)
      }
  }
}

sealed abstract class AtomicExpr[MT <: MetaTypes] extends Expr[MT] with Product { this: HashedExpr =>
  override val position: AtomicPositionInfo
}

/********* Column *********/

final case class Column[MT <: MetaTypes](
  table: TableLabel[MT#DatabaseTableNameImpl],
  column: ColumnLabel[MT#DatabaseColumnNameImpl],
  typ: MT#CT
)(
  val position: AtomicPositionInfo
) extends
    AtomicExpr[MT]
    with expression.ColumnImpl[MT]
    with HashedExpr
object Column extends expression.OColumnImpl

/********* Select list reference *********/

final case class SelectListReference[MT <: MetaTypes](
  index: Int,
  isAggregated: Boolean,
  isWindowed: Boolean,
  typ: MT#CT
)(
  val position: AtomicPositionInfo
) extends
    AtomicExpr[MT]
    with expression.SelectListReferenceImpl[MT]
    with HashedExpr
object SelectListReference extends expression.OSelectListReferenceImpl

/********* Literal *********/

sealed abstract class Literal[MT <: MetaTypes]
    extends AtomicExpr[MT]
    with expression.LiteralImpl[MT] { this: HashedExpr =>
}

/********* Literal value *********/

final case class LiteralValue[MT <: MetaTypes](
  value: MT#CV
)(
  val position: AtomicPositionInfo
)(
  implicit ev: HasType[MT#CV, MT#CT]
) extends
    Literal[MT]
    with expression.LiteralValueImpl[MT]
    with HashedExpr
{
  // these need to be here and not in the impl for variance reasons
  val typ = ev.typeOf(value)
  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))
}
object LiteralValue extends expression.OLiteralValueImpl

/********* Null literal *********/

final case class NullLiteral[MT <: MetaTypes](
  typ: MT#CT
)(
  val position: AtomicPositionInfo
) extends
    Literal[MT]
    with expression.NullLiteralImpl[MT]
    with HashedExpr
object NullLiteral extends expression.ONullLiteralImpl

/********* FuncallLike *********/

sealed abstract class FuncallLike[MT <: MetaTypes] extends Expr[MT] with Product { this: HashedExpr =>
  override val position: FuncallPositionInfo
}

/********* Function call *********/

final case class FunctionCall[MT <: MetaTypes](
  function: MonomorphicFunction[MT#CT],
  args: Seq[Expr[MT]]
)(
  val position: FuncallPositionInfo
) extends
    FuncallLike[MT]
    with expression.FunctionCallImpl[MT]
    with HashedExpr
object FunctionCall extends expression.OFunctionCallImpl

/********* Aggregate function call *********/

final case class AggregateFunctionCall[MT <: MetaTypes](
  function: MonomorphicFunction[MT#CT],
  args: Seq[Expr[MT]],
  distinct: Boolean,
  filter: Option[Expr[MT]]
)(
  val position: FuncallPositionInfo
) extends
    FuncallLike[MT]
    with expression.AggregateFunctionCallImpl[MT]
    with HashedExpr
{
  require(function.isAggregate)
}
object AggregateFunctionCall extends expression.OAggregateFunctionCallImpl

/********* Windowed function call *********/

final case class WindowedFunctionCall[MT <: MetaTypes](
  function: MonomorphicFunction[MT#CT],
  args: Seq[Expr[MT]],
  filter: Option[Expr[MT]],
  partitionBy: Seq[Expr[MT]], // is normal right here, or should it be aggregate?
  orderBy: Seq[OrderBy[MT]], // ditto thus
  frame: Option[Frame]
)(
  val position: FuncallPositionInfo
) extends
    FuncallLike[MT]
    with expression.WindowedFunctionCallImpl[MT]
    with HashedExpr
{
  require(function.needsWindow || function.isAggregate)
}

object WindowedFunctionCall extends expression.OWindowedFunctionCallImpl
