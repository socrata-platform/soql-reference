package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.FunctionInfo

import DocUtils._

trait AggregateFunctionCallImpl[MT <: MetaTypes] { this: AggregateFunctionCall[MT] =>
  type Self[MT <: MetaTypes] = AggregateFunctionCall[MT]

  val typ = function.result
  def isAggregated = true
  val isWindowed = false

  val size = 1 + args.iterator.map(_.size).sum + filter.fold(0)(_.size)

  private[analyzer2] lazy val columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = {
    var refs =
      args.foldLeft(Map.empty[AutoTableLabel, Set[ColumnLabel]]) { (acc, arg) =>
        acc.mergeWith(arg.columnReferences)(_ ++ _)
      }
    for(f <- filter) {
      refs = refs.mergeWith(f.columnReferences)(_ ++ _)
    }
    refs
  }

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    Some(this).filter(predicate).orElse {
      args.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      filter.flatMap(_.find(predicate))
    }

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean =
    that match {
      case AggregateFunctionCall(thatFunction, thatArgs, thatDistinct, thatFilter) =>
        this.function == thatFunction &&
          this.args.length == thatArgs.length &&
          this.args.zip(thatArgs).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.distinct == thatDistinct &&
          this.filter.isDefined == thatFilter.isDefined &&
          this.filter.zip(thatFilter).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.filter.zip(thatFilter).forall { case (a, b) => a.findIsomorphism(state, b) }
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]): Self[MT2] =
    AggregateFunctionCall(
      function = state.changesOnlyLabels.convertCTOnly(function),
      args = args.map(_.doRewriteDatabaseNames(state)),
      filter = filter.map(_.doRewriteDatabaseNames(state)),
      distinct = distinct
    )(state.convert(position))

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)),
         filter = filter.map(_.doRelabel(state)))(position)

  protected def doDebugDoc(implicit ev: StatementDocProvider[MT]) = {
    val preArgs = Seq(
      Some(Doc(function.name.name)),
      Some(d"("),
      if(distinct) Some(d"DISTINCT ") else None
    ).flatten.hcat
    val postArgs = Seq(
      Some(d")"),
      filter.map { w => w.debugDoc(ev).encloseNesting(d"FILTER (", d")") }
    ).flatten.hsep
    args.map(_.debugDoc(ev)).encloseNesting(preArgs, d",", postArgs)
  }

  private[analyzer2] def reReference(reference: Source): Self[MT] =
    copy()(position = position.reReference(reference))
}

trait OAggregateFunctionCallImpl { this: AggregateFunctionCall.type =>
  implicit def serialize[MT <: MetaTypes](implicit expr: Writable[Expr[MT]], mf: Writable[MonomorphicFunction[MT#ColumnType]], rns: Writable[MT#ResourceNameScope]) = new Writable[AggregateFunctionCall[MT]] {
    def writeTo(buffer: WriteBuffer, afc: AggregateFunctionCall[MT]): Unit = {
      buffer.write(afc.function)
      buffer.write(afc.args)
      buffer.write(afc.distinct)
      buffer.write(afc.filter)
      buffer.write(afc.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit expr: Readable[Expr[MT]], mf: Readable[MonomorphicFunction[MT#ColumnType]], rns: Readable[MT#ResourceNameScope]) = new Readable[AggregateFunctionCall[MT]] with ExpressionUniverse[MT] {
    def readFrom(buffer: ReadBuffer): AggregateFunctionCall = {
      val function = buffer.read[MonomorphicFunction]()
      val args = buffer.read[Seq[Expr]]()
      val distinct = buffer.read[Boolean]()
      val filter = buffer.read[Option[Expr]]()
      val position = buffer.read[FuncallPositionInfo]()
      AggregateFunctionCall(
        function,
        args,
        distinct,
        filter
      )(
        position
      )
    }
  }
}
