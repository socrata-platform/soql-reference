package com.socrata.soql.analyzer2.from

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

import DocUtils._

trait FromStatementImpl[+RNS, +CT, +CV] { this: FromStatement[RNS, CT, CV] =>
  type Self[+RNS, +CT, +CV] = FromStatement[RNS, CT, CV]
  def asSelf = this

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = statement.columnReferences

  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]]): Self[RNS, CT, CV] = {
    copy(statement = statement.doRemoveUnusedColumns(used, Some(label)))
  }

  def find(predicate: Expr[CT, CV] => Boolean) = statement.find(predicate)
  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    statement.contains(e)

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromStatement(thatStatement, thatLabel, thatAlias) =>
        state.tryAssociate(this.label, thatLabel) &&
          this.statement.findIsomorphism(state, Some(this.label), Some(thatLabel), thatStatement)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(statement = statement.doRewriteDatabaseNames(state))

  def useSelectListReferences = copy(statement = statement.useSelectListReferences)

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(statement = statement.doRelabel(state),
         label = state.convert(label))
  }

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): FromStatement[RNS2, CT, CV] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV] =
    copy(statement = statement.mapAlias(f), alias = f(alias))

  private[analyzer2] def realTables = statement.realTables

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, CV]) = {
    val (orderColumn, stmt) =
      statement.preserveOrdering(provider, rowNumberFunction, wantOutputOrdered, wantOrderingColumn)

    (orderColumn.map((label, _)), copy(statement = stmt))
  }

  def debugDoc(implicit ev: HasDoc[CV]) =
    (statement.debugDoc.encloseNesting(d"(", d")") +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}
