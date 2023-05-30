package com.socrata.soql.analyzer2.statement

import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.functions.MonomorphicFunction

import DocUtils._

trait ValuesImpl[MT <: MetaTypes] { this: Values[MT] =>
  type Self[MT <: MetaTypes] = Values[MT]
  def asSelf = this

  def unique = if(values.tail.isEmpty) LazyList(Nil) else LazyList.empty

  val schema: OrderedMap[AutoColumnLabel, Statement.SchemaEntry[MT]] =
    OrderedMap() ++ values.head.iterator.zip(labels.iterator).zipWithIndex.map { case ((expr, label), idx) =>
      label -> Statement.SchemaEntry(ColumnName(s"column_${idx+1}"), expr.typ)
    }

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    Map.empty

  private[analyzer2] def findIsomorphism(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT]
  ): Boolean =
    that match {
      case Values(thatLabels, thatValues) =>
        this.values.length == thatValues.length &&
        this.labels.iterator.zip(thatLabels.iterator).forall { case (l1, l2) =>
          state.tryAssociate(thisCurrentTableLabel, l1, thatCurrentTableLabel, l2)
        } &&
        this.schema.size == that.schema.size &&
          this.values.iterator.zip(thatValues.iterator).forall { case (thisRow, thatRow) =>
            thisRow.iterator.zip(thatRow.iterator).forall { case (thisExpr, thatExpr) =>
              thisExpr.findIsomorphism(state, thatExpr)
            }
          }
      case _ =>
        false
    }

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    values.iterator.flatMap(_.iterator.flatMap(_.find(predicate))).nextOption()

  def contains(e: Expr[MT]): Boolean =
    values.exists(_.exists(_.contains(e)))

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]) = this

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] =
    set

  private[analyzer2] def realTables = Map.empty[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy(
      values = values.map(_.map(_.doRewriteDatabaseNames(state)))
    )

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT] =
    copy(values = values.map(_.map(_.doRelabel(state))))

  private[analyzer2] override def doDebugDoc(implicit ev: StatementDocProvider[MT]): Doc[Annotation[MT]] = {
    Seq(
      d"VALUES",
      values.toSeq.map { row =>
        row.toSeq.zip(schema.keys).
          map { case (expr, label) =>
            expr.debugDoc(ev).annotate(Annotation.ColumnAliasDefinition(schema(label).name, label))
          }.encloseNesting(d"(", d",", d")")
      }.encloseNesting(d"(", d",", d")")
    ).sep.nest(2)
  }

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    // no interior queries, nothing to do
  }
}

trait OValuesImpl { this: Values.type =>
  implicit def serialize[MT <: MetaTypes](implicit ev: Writable[Expr[MT]]): Writable[Values[MT]] =
    new Writable[Values[MT]] {
      def writeTo(buffer: WriteBuffer, values: Values[MT]): Unit = {
        buffer.write(values.labels)
        buffer.write(values.values)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit ev: Readable[Expr[MT]]): Readable[Values[MT]] =
    new Readable[Values[MT]] {
      def readFrom(buffer: ReadBuffer): Values[MT] = {
        Values(
          labels = buffer.read[OrderedSet[AutoColumnLabel]](),
          values = buffer.read[NonEmptySeq[NonEmptySeq[Expr[MT]]]]()
        )
      }
    }
}
