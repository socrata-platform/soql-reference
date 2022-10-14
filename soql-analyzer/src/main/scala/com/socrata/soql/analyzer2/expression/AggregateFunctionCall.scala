package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{FunctionInfo, HasDoc, HasType}

import DocUtils._

trait AggregateFunctionCallImpl[+CT, +CV] { this: AggregateFunctionCall[CT, CV] =>
  type Self[+CT, +CV] = AggregateFunctionCall[CT, CV]

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

trait OAggregateFunctionCallImpl { this: AggregateFunctionCall.type =>
  implicit def serialize[CT: Writable, CV: Writable] = new Writable[AggregateFunctionCall[CT, CV]] {
    def writeTo(buffer: WriteBuffer, afc: AggregateFunctionCall[CT, CV]): Unit = {
      buffer.write(afc.function)
      buffer.write(afc.args)
      buffer.write(afc.distinct)
      buffer.write(afc.filter)
      buffer.write(afc.position)
      buffer.write(afc.functionNamePosition)
    }
  }

  implicit def deserialize[CT: Readable, CV](implicit mf: Readable[MonomorphicFunction[CT]], e: Readable[Expr[CT, CV]]) = new Readable[AggregateFunctionCall[CT, CV]] {
    def readFrom(buffer: ReadBuffer): AggregateFunctionCall[CT, CV] = {
      val function = buffer.read[MonomorphicFunction[CT]]()
      val args = buffer.read[Seq[Expr[CT, CV]]]()
      val distinct = buffer.read[Boolean]()
      val filter = buffer.read[Option[Expr[CT, CV]]]()
      val position = buffer.read[Position]()
      val functionNamePosition = buffer.read[Position]()
      AggregateFunctionCall(
        function,
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
