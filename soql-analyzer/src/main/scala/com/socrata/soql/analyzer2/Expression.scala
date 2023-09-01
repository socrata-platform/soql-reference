package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.Pretty

import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.FunctionInfo
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

import DocUtils._

private[analyzer2] trait HashedExpr { this: Product =>
  // Since this is completely immutable, cache the hashCode rather
  // than recomputing, as these trees can be quite deep.  This is a
  // mixin so it can be computed after the various case classes are
  // fully constructed.
  override final val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)
}

sealed abstract class Expr[MT <: MetaTypes] extends Product with LabelUniverse[MT] { this: HashedExpr =>
  type Self[MT <: MetaTypes] <: Expr[MT]

  val typ: CT
  val position: PositionInfo

  val size: Int

  def isAggregated: Boolean
  def isWindowed: Boolean

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](expr: RewriteDatabaseNamesState[MT2]): Self[MT2]

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT]

  private[analyzer2] def reposition(p: Position): Self[MT]

  final def isIsomorphic(that: Expr[MT]): Boolean = findIsomorphism(new IsomorphismState, that)
  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean
  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]]

  final def debugStr(implicit ev: HasDoc[CV], ev2: HasDoc[MT#DatabaseColumnNameImpl]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV], ev2: HasDoc[MT#DatabaseColumnNameImpl]): StringBuilder = debugDoc(ev, ev2).layoutSmart().toStringBuilder(sb)
  final def debugDoc(implicit ev: HasDoc[CV], ev2: HasDoc[MT#DatabaseColumnNameImpl]): Doc[Annotation[MT]] =
    debugDoc(new ExprDocProvider(ev, ev2))
  private[analyzer2] final def debugDoc(implicit ev: ExprDocProvider[MT]): Doc[Annotation[MT]] =
    doDebugDoc.annotate(Annotation.Typed(typ))
  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]): Doc[Annotation[MT]]

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]]

  final def contains(e: Expr[MT]): Boolean = find(_ == e).isDefined
}
object Expr {
  implicit def serialize[MT <: MetaTypes](implicit writableCT: Writable[MT#ColumnType], writableCV: Writable[MT#ColumnValue], writableDTN: Writable[MT#DatabaseTableNameImpl], writableDCN: Writable[MT#DatabaseColumnNameImpl]): Writable[Expr[MT]] = new Writable[Expr[MT]] with ExpressionUniverse[MT] {
    implicit val self = this
    def writeTo(buffer: WriteBuffer, t: Expr): Unit =
      t match {
        case c : PhysicalColumn =>
          buffer.write(0)
          buffer.write(c)
        case c : VirtualColumn =>
          buffer.write(1)
          buffer.write(c)
        case slr: SelectListReference =>
          buffer.write(2)
          buffer.write(slr)
        case lv: LiteralValue =>
          buffer.write(3)
          buffer.write(lv)
        case nl: NullLiteral =>
          buffer.write(4)
          buffer.write(nl)
        case fc: FunctionCall =>
          buffer.write(5)
          buffer.write(fc)
        case afc: AggregateFunctionCall =>
          buffer.write(6)
          buffer.write(afc)
        case wfc: WindowedFunctionCall =>
          buffer.write(7)
          buffer.write(wfc)
      }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableCT: Readable[MT#ColumnType], readableCV: Readable[MT#ColumnValue], hasType: HasType[MT#ColumnValue, MT#ColumnType], mf: Readable[MonomorphicFunction[MT#ColumnType]], readableDTN: Readable[MT#DatabaseTableNameImpl], readableDCN: Readable[MT#DatabaseColumnNameImpl]): Readable[Expr[MT]] = new Readable[Expr[MT]] with ExpressionUniverse[MT] {
    implicit val self = this
    def readFrom(buffer: ReadBuffer): Expr =
      buffer.read[Int]() match {
        case 0 => buffer.read[PhysicalColumn]()
        case 1 => buffer.read[VirtualColumn]()
        case 2 => buffer.read[SelectListReference]()
        case 3 => buffer.read[LiteralValue]()
        case 4 => buffer.read[NullLiteral]()
        case 5 => buffer.read[FunctionCall]()
        case 6 => buffer.read[AggregateFunctionCall]()
        case 7 => buffer.read[WindowedFunctionCall]()
        case other => fail("Unknown expression tag " + other)
      }
  }
}

sealed abstract class AtomicExpr[MT <: MetaTypes] extends Expr[MT] with Product { this: HashedExpr =>
  type Self[MT <: MetaTypes] <: AtomicExpr[MT]

  override val position: AtomicPositionInfo
}

/********* Column *********/

sealed abstract class Column[MT <: MetaTypes] extends AtomicExpr[MT] { this: HashedExpr =>
  type Self[MT <: MetaTypes] <: Column[MT]

  val table: AutoTableLabel
  val column: ColumnLabel
}

final case class PhysicalColumn[MT <: MetaTypes](
  table: AutoTableLabel,
  tableName: types.DatabaseTableName[MT],
  tableCanonicalName: CanonicalName,
  column: types.DatabaseColumnName[MT],
  typ: MT#ColumnType
)(
  val position: AtomicPositionInfo
) extends
    Column[MT]
    with expression.PhysicalColumnImpl[MT]
    with HashedExpr
object PhysicalColumn extends expression.OPhysicalColumnImpl

final case class VirtualColumn[MT <: MetaTypes](
  table: AutoTableLabel,
  column: AutoColumnLabel,
  typ: MT#ColumnType
)(
  val position: AtomicPositionInfo
) extends
    Column[MT]
    with expression.VirtualColumnImpl[MT]
    with HashedExpr
object VirtualColumn extends expression.OVirtualColumnImpl

/********* Select list reference *********/

final case class SelectListReference[MT <: MetaTypes](
  index: Int,
  isAggregated: Boolean,
  isWindowed: Boolean,
  typ: MT#ColumnType
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
  value: MT#ColumnValue
)(
  val position: AtomicPositionInfo
)(
  implicit ev: HasType[MT#ColumnValue, MT#ColumnType]
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
  typ: MT#ColumnType
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
  function: MonomorphicFunction[MT#ColumnType],
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
  function: MonomorphicFunction[MT#ColumnType],
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
  function: MonomorphicFunction[MT#ColumnType],
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
