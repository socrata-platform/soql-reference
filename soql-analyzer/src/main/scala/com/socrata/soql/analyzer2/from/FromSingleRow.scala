package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait FromSingleRowImpl[MT <: MetaTypes] { this: FromSingleRow[MT] =>
  type Self[MT <: MetaTypes] = FromSingleRow[MT]
  def asSelf = this

  val resourceName = None

  // We have a unique ordering and no columns are required to achieve it
  def unique = LazyList(Nil)

  private[analyzer2] val scope: Scope[Nothing] =
    Scope(
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  def find(predicate: Expr[CT, CV] => Boolean) = None
  def contains[CT, CV](e: Expr[CT, CV]): Boolean = false

  private[analyzer2] final def findIsomorphism(state: IsomorphismState, that: From[MT]): Boolean =
    that match {
      case FromSingleRow(thatLabel, thatAlias) =>
        state.tryAssociate(this.label, thatLabel)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT] = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromSingleRow[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(alias = f(alias))

  private[analyzer2] def realTables = Map.empty[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
    state.tableMap += label -> LabelMap.TableReference(resourceName, alias)
    // no columns
  }

  def debugDoc(implicit ev: HasDoc[CV]) =
    (d"(SELECT)" +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}

trait OFromSingleRowImpl { this: FromSingleRow.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWRitable: Writable[MT#RNS]): Writable[FromSingleRow[MT]] =
    new Writable[FromSingleRow[MT]] {
      def writeTo(buffer: WriteBuffer, fsr: FromSingleRow[MT]): Unit = {
        buffer.write(fsr.label)
        buffer.write(fsr.alias)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#RNS]): Readable[FromSingleRow[MT]] =
    new Readable[FromSingleRow[MT]] {
      def readFrom(buffer: ReadBuffer): FromSingleRow[MT] = {
        FromSingleRow(
          label = buffer.read[AutoTableLabel](),
          alias = buffer.read[Option[ResourceName]]()
        )
      }
    }
}
