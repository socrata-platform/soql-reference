package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.FunctionInfo

trait FunctionCallImpl[MT <: MetaTypes] { this: FunctionCall[MT] =>
  type Self[MT <: MetaTypes] = FunctionCall[MT]

  require(!function.isAggregate)
  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum
  val isAggregated = args.exists(_.isAggregated)
  val isWindowed = args.exists(_.isWindowed)

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    args.foldLeft(Map.empty[AutoTableLabel, Set[ColumnLabel]]) { (acc, arg) =>
      acc.mergeWith(arg.columnReferences)(_ ++ _)
    }

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    Some(this).filter(predicate).orElse {
      args.iterator.flatMap(_.find(predicate)).nextOption()
    }

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean =
    that match {
      case FunctionCall(thatFunction, thatArgs) =>
        this.function == thatFunction &&
          this.args.length == thatArgs.length &&
          this.args.zip(thatArgs).forall { case (a, b) => a.findIsomorphism(state, b) }
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    this.copy(
      function = state.changesOnlyLabels.convertCTOnly(function),
      args = args.map(_.doRewriteDatabaseNames(state))
    )(position)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)))(position)

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) =
    args.map(_.debugDoc(ev)).encloseHanging(Doc(function.name.name) ++ d"(", d",", d")")

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))
}

trait OFunctionCallImpl { this: FunctionCall.type =>
  implicit def serialize[MT <: MetaTypes](implicit writableExpr: Writable[Expr[MT]], mf: Writable[MonomorphicFunction[MT#ColumnType]]): Writable[FunctionCall[MT]] = new Writable[FunctionCall[MT]] {
    def writeTo(buffer: WriteBuffer, fc: FunctionCall[MT]): Unit = {
      buffer.write(fc.function)
      buffer.write(fc.args)
      buffer.write(fc.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit readableExpr: Readable[Expr[MT]], mf: Readable[MonomorphicFunction[MT#ColumnType]]): Readable[FunctionCall[MT]] = new Readable[FunctionCall[MT]] with ExpressionUniverse[MT] {
    def readFrom(buffer: ReadBuffer): FunctionCall = {
      val function = buffer.read[MonomorphicFunction]()
      val args = buffer.read[Seq[Expr]]()
      val position = buffer.read[FuncallPositionInfo]()
      FunctionCall(function, args)(position)
    }
  }
}
