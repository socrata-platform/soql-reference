package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait FromSingleRowImpl[+RNS] { this: FromSingleRow[RNS] =>
  type Self[+RNS, +CT, +CV] = FromSingleRow[RNS]
  def asSelf = this

  private[analyzer2] val scope: Scope[Nothing] =
    Scope(
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  def useSelectListReferences = this
  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]]) = this

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

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): FromSingleRow[RNS2] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, Nothing, Nothing] =
    copy(alias = f(alias))

  private[analyzer2] def realTables = Map.empty[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] override def preserveOrdering[CT2](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, Nothing, Nothing]) =
    (None, this)

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (d"(SELECT)" +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}
