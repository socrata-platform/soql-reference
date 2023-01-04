package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait FromSingleRowImpl[+RNS] { this: FromSingleRow[RNS] =>
  type Self[+RNS, +CT, +CV] = FromSingleRow[RNS]
  def asSelf = this

  val resourceName = None

  // We have a unique ordering and no columns are required to achieve it
  def unique = Some(Nil)

  private[analyzer2] val scope: Scope[Nothing] =
    Scope(
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  def find(predicate: Expr[Nothing, Nothing] => Boolean) = None
  def contains[CT, CV](e: Expr[CT, CV]): Boolean = false

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    that match {
      case FromSingleRow(thatLabel, thatAlias) =>
        state.tryAssociate(this.label, thatLabel)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromSingleRow[RNS] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[RNS, Nothing, Nothing] =
    copy(alias = f(alias))

  private[analyzer2] def realTables = Map.empty[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
    state.tableMap += label -> LabelMap.TableReference(resourceName, alias)
    // no columns
  }

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (d"(SELECT)" +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}

trait OFromSingleRowImpl { this: FromSingleRow.type =>
  implicit def serialize[RNS: Writable]: Writable[FromSingleRow[RNS]] =
    new Writable[FromSingleRow[RNS]] {
      def writeTo(buffer: WriteBuffer, fsr: FromSingleRow[RNS]): Unit = {
        buffer.write(fsr.label)
        buffer.write(fsr.alias)
      }
    }

  implicit def deserialize[RNS: Readable]: Readable[FromSingleRow[RNS]] =
    new Readable[FromSingleRow[RNS]] {
      def readFrom(buffer: ReadBuffer): FromSingleRow[RNS] = {
        FromSingleRow(
          label = buffer.read[AutoTableLabel](),
          alias = buffer.read[Option[ResourceName]]()
        )
      }
    }
}
