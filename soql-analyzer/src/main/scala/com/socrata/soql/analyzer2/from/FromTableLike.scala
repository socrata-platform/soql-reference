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

trait FromTableLikeImpl[+RNS, +CT] { this: FromTableLike[RNS, CT] =>
  type Self[+RNS, +CT, +CV] <: FromTableLike[RNS, CT]

  val tableName: TableLabel
  val label: TableLabel
  val columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]

  def find(predicate: Expr[CT, Nothing] => Boolean) = None
  def contains[CT2 >: CT, CV](e: Expr[CT2, CV]): Boolean =
    false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] override final val scope: Scope[CT] = Scope(columns, label)

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, Nothing]) =
    (None, asSelf)

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (tableName.debugDoc ++ Doc.softlineSep ++ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}
