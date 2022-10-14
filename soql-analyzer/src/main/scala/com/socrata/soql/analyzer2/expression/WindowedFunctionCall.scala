package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.{FunctionInfo, HasDoc, HasType}

import DocUtils._

trait WindowedFunctionCallImpl[+CT, +CV] { this: WindowedFunctionCall[CT, CV] =>
  type Self[+CT, +CV] = WindowedFunctionCall[CT, CV]

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

trait OWindowedFunctionCallImpl { this: WindowedFunctionCall.type =>
  implicit def serialize[CT: Writable, CV: Writable] = new Writable[WindowedFunctionCall[CT, CV]] {
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

  implicit def deserialize[CT: Readable, CV: Readable](implicit ht: HasType[CV, CT], fi: FunctionInfo[CT]) = new Readable[WindowedFunctionCall[CT, CV]] {
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
