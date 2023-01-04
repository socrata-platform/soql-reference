package com.socrata.soql.analyzer2.statement

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

import DocUtils._

trait ValuesImpl[+CT, +CV] { this: Values[CT, CV] =>
  type Self[+RNS, +CT, +CV] = Values[CT, CV]
  def asSelf = this

  def unique = None

  // This lets us see the schema with DatabaseColumnNames as keys
  def typeVariedSchema[T >: DatabaseColumnName]: OrderedMap[T, NameEntry[CT]] =
    OrderedMap() ++ values.head.iterator.zipWithIndex.map { case (expr, idx) =>
      // This is definitely a postgresqlism, unfortunately
      val name = s"column${idx+1}"
      DatabaseColumnName(name) -> NameEntry(ColumnName(name), expr.typ)
    }

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map.empty

  private[analyzer2] def findIsomorphism[RNS2, CT2 >: CT, CV2 >: CV](
    state: IsomorphismState,
    thisCurrentTableLabel: Option[TableLabel],
    thatCurrentTableLabel: Option[TableLabel],
    that: Statement[RNS2, CT2, CV2]
  ): Boolean =
    that match {
      case Values(thatValues) =>
        this.values.length == thatValues.length &&
        this.schema.size == that.schema.size &&
          this.values.iterator.zip(thatValues.iterator).forall { case (thisRow, thatRow) =>
            thisRow.iterator.zip(thatRow.iterator).forall { case (thisExpr, thatExpr) =>
              thisExpr.findIsomorphism(state, thatExpr)
            }
          }
      case _ =>
        false
    }

  val schema = typeVariedSchema

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    values.iterator.flatMap(_.iterator.flatMap(_.find(predicate))).nextOption()

  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    values.exists(_.exists(_.contains(e)))

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]) = this

  private[analyzer2] def realTables = Map.empty[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      values = values.map(_.map(_.doRewriteDatabaseNames(state)))
    )

  private[analyzer2] def doRelabel(state: RelabelState): Self[Nothing, CT, CV] =
    copy(values = values.map(_.map(_.doRelabel(state))))

  override def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[Nothing, CT]] = {
    Seq(
      d"VALUES",
      values.toSeq.map { row =>
        row.toSeq.zip(schema.keys).
          map { case (expr, label) =>
            expr.debugDoc.annotate(Annotation.ColumnAliasDefinition(schema(label).name, label))
          }.encloseNesting(d"(", d",", d")")
      }.encloseNesting(d"(", d",", d")")
    ).sep.nest(2)
  }

  private[analyzer2] def doLabelMap[RNS](state: LabelMapState[RNS]): Unit = {
    // no interior queries, nothing to do
  }
}

trait OValuesImpl { this: Values.type =>
  implicit def serialize[CT, CV](implicit ev: Writable[Expr[CT, CV]]): Writable[Values[CT, CV]] =
    new Writable[Values[CT, CV]] {
      def writeTo(buffer: WriteBuffer, values: Values[CT, CV]): Unit = {
        buffer.write(values.values)
      }
    }

  implicit def deserialize[CT, CV](implicit ev: Readable[Expr[CT, CV]]): Readable[Values[CT, CV]] =
    new Readable[Values[CT, CV]] {
      def readFrom(buffer: ReadBuffer): Values[CT, CV] = {
        Values(buffer.read[NonEmptySeq[NonEmptySeq[Expr[CT, CV]]]]())
      }
    }
}
