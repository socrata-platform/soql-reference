package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{FunctionInfo, HasDoc, HasType}

trait FunctionCallImpl[+CT, +CV] { this: FunctionCall[CT, CV] =>
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

trait OFunctionCallImpl { this: FunctionCall.type =>
  implicit def serialize[CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]) = new Writable[FunctionCall[CT, CV]] {
    def writeTo(buffer: WriteBuffer, fc: FunctionCall[CT, CV]): Unit = {
      buffer.write(fc.function)
      buffer.write(fc.args)
      buffer.write(fc.position)
      buffer.write(fc.functionNamePosition)
    }
  }

  implicit def deserialize[CT: Readable, CV](implicit fi: Readable[MonomorphicFunction[CT]], e: Readable[Expr[CT, CV]]) = new Readable[FunctionCall[CT, CV]] {
    def readFrom(buffer: ReadBuffer): FunctionCall[CT, CV] = {
      val function = buffer.read[MonomorphicFunction[CT]]()
      val args = buffer.read[Seq[Expr[CT, CV]]]()
      val position = buffer.read[Position]()
      val functionNamePosition = buffer.read[Position]()
      FunctionCall(function, args)(position, functionNamePosition)
    }
  }
}
