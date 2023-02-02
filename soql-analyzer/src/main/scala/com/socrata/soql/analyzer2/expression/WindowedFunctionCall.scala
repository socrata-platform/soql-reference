package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{FunctionInfo, HasDoc, HasType}

import DocUtils._

trait WindowedFunctionCallImpl[MT <: MetaTypes] { this: WindowedFunctionCall[MT] =>
  type Self[MT <: MetaTypes] = WindowedFunctionCall[MT]

  val typ = function.result

  val size = 1 + args.iterator.map(_.size).sum + partitionBy.iterator.map(_.size).sum + orderBy.iterator.map(_.expr.size).sum

  def isAggregated = args.exists(_.isAggregated) || partitionBy.exists(_.isAggregated) || orderBy.exists(_.expr.isAggregated)
  def isWindowed = true

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = {
    var refs =
      args.foldLeft(Map.empty[AutoTableLabel, Set[ColumnLabel]]) { (acc, arg) =>
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

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    Some(this).filter(predicate).orElse {
      args.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      filter.flatMap(_.find(predicate))
    }.orElse {
      partitionBy.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      orderBy.iterator.flatMap(_.expr.find(predicate)).nextOption()
    }

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean =
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

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy[MT2](
      function = state.changesOnlyLabels.convertCTOnly(function),
      args = args.map(_.doRewriteDatabaseNames(state)),
      filter = filter.map(_.doRewriteDatabaseNames(state)),
      partitionBy = partitionBy.map(_.doRewriteDatabaseNames(state)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(state))
    )(position)

  private[analyzer2] def doRelabel(state: RelabelState) =
    copy(args = args.map(_.doRelabel(state)),
         partitionBy = partitionBy.map(_.doRelabel(state)),
         orderBy = orderBy.map(_.doRelabel(state)))(position)

  protected def doDebugDoc(implicit ev: ExprDocProvider[MT]) = {
    val preArgs: Doc[Nothing] = Doc(function.name.name) ++ d"("
    val windowParts: Doc[Annotation[MT]] =
      Seq[Option[Doc[Annotation[MT]]]](
        if(partitionBy.nonEmpty) {
          Some((d"PARTITION BY" +: partitionBy.map(_.debugDoc(ev)).punctuate(d",")).sep.nest(2))
        } else {
          None
        },
        if(orderBy.nonEmpty) {
          Some((d"ORDER BY" +: orderBy.map(_.debugDoc(ev)).punctuate(d",")).sep.nest(2))
        } else {
          None
        },
        frame.map(_.debugDoc)
      ).flatten.sep.encloseNesting(d"(", d")")
    val postArgs = Seq(
      Some(d")"),
      filter.map { w => w.debugDoc(ev).encloseNesting(d"FILTER (", d")") },
      Some(d"OVER" +#+ windowParts)
    ).flatten.hsep
    args.map(_.debugDoc(ev)).encloseNesting(preArgs, d",", postArgs)
  }

  private[analyzer2] def reposition(p: Position): Self[MT] = copy()(position = position.logicallyReposition(p))
}

trait OWindowedFunctionCallImpl { this: WindowedFunctionCall.type =>
  implicit def serialize[MT <: MetaTypes](implicit expr: Writable[Expr[MT]], mf: Writable[MonomorphicFunction[MT#ColumnType]]) = new Writable[WindowedFunctionCall[MT]] {
    def writeTo(buffer: WriteBuffer, wfc: WindowedFunctionCall[MT]): Unit = {
      buffer.write(wfc.function)
      buffer.write(wfc.args)
      buffer.write(wfc.filter)
      buffer.write(wfc.partitionBy)
      buffer.write(wfc.orderBy)
      buffer.write(wfc.frame)(Writable.option(Frame.serialize))
      buffer.write(wfc.position)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit expr: Readable[Expr[MT]], mf: Readable[MonomorphicFunction[MT#ColumnType]]): Readable[WindowedFunctionCall[MT]] = new Readable[WindowedFunctionCall[MT]] with ExpressionUniverse[MT] {
    def readFrom(buffer: ReadBuffer): WindowedFunctionCall = {
      val function = buffer.read[MonomorphicFunction]()
      val args = buffer.read[Seq[Expr]]()
      val filter = buffer.read[Option[Expr]]()
      val partitionBy = buffer.read[Seq[Expr]]()
      val orderBy = buffer.read[Seq[OrderBy]]()
      val frame = buffer.read[Option[Frame]]()(Readable.option(Frame.serialize))
      val position = buffer.read[FuncallPositionInfo]()
      WindowedFunctionCall(
        function,
        args,
        filter,
        partitionBy,
        orderBy,
        frame
      )(
        position
      )
    }
  }
}
