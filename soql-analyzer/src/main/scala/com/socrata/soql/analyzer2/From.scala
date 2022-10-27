package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

import DocUtils._

sealed abstract class From[+RNS, +CT, +CV] {
  type Self[+RNS, +CT, +CV] <: From[RNS, CT, CV]
  def asSelf: Self[RNS, CT, CV]

  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Either[AddScopeError, Environment[CT2]]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Self[RNS, CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Self[RNS, CT, CV]

  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]]): Self[RNS, CT, CV]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean
  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]]

  private[analyzer2] def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, CV])

  def useSelectListReferences: Self[RNS, CT, CV]

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]]
  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder = debugDoc.layoutSmart().toStringBuilder(sb)
  def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[RNS, CT]]

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV]

  type ReduceResult[+RNS, +CT, +CV] <: From[RNS, CT, CV]

  def reduceMap[S, RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => (S, AtomicFrom[RNS2, CT2, CV2]),
    combine: (S, JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => (S, Join[RNS2, CT2, CV2])
  ): (S, ReduceResult[RNS2, CT2, CV2])

  final def map[RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => AtomicFrom[RNS2, CT2, CV2],
    combine: (JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => Join[RNS2, CT2, CV2]
  ): ReduceResult[RNS2, CT2, CV2] = {
    reduceMap[Unit, RNS2, CT2, CV2](
      { nonJoin => ((), base.apply(nonJoin)) },
      { (_, joinType, lateral, left, right, on) => ((), combine(joinType, lateral, left, right, on)) }
    )._2
  }

  final def reduce[S](
    base: AtomicFrom[RNS, CT, CV] => S,
    combine: (S, Join[RNS, CT, CV]) => S
  ): S =
    reduceMap[S, RNS, CT, CV](
      { nonJoin => (base(nonJoin), nonJoin) },
      { (s, joinType, lateral, left, right, on) =>
        val j = Join(joinType, lateral, left, right, on)
        (combine(s, j), j)
      }
    )._1
}

object From {
  implicit def serialize[RNS: Writable, CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]): Writable[From[RNS, CT, CV]] = new Writable[From[RNS, CT, CV]] {
    def writeTo(buffer: WriteBuffer, from: From[RNS, CT, CV]): Unit = {
      from match {
        case j: Join[RNS, CT, CV] =>
          buffer.write(0)
          buffer.write(j)
        case ft: FromTable[RNS, CT] =>
          buffer.write(1)
          buffer.write(ft)
        case fs: FromStatement[RNS, CT, CV] =>
          buffer.write(2)
          buffer.write(fs)
        case fsr: FromSingleRow[RNS] =>
          buffer.write(3)
          buffer.write(fsr)
      }
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable, CV](implicit ev: Readable[Expr[CT, CV]]): Readable[From[RNS, CT, CV]] = new Readable[From[RNS, CT, CV]] {
    def readFrom(buffer: ReadBuffer): From[RNS, CT, CV] = {
      buffer.read[Int]() match {
        case 0 => buffer.read[Join[RNS, CT, CV]]()
        case 1 => buffer.read[FromTable[RNS, CT]]()
        case 2 => buffer.read[FromStatement[RNS, CT, CV]]()
        case 3 => buffer.read[FromSingleRow[RNS]]()
        case other => fail("Unknown from tag " + other)
      }
    }
  }
}

case class Join[+RNS, +CT, +CV](
  joinType: JoinType,
  lateral: Boolean,
  left: From[RNS, CT, CV],
  right: AtomicFrom[RNS, CT, CV],
  on: Expr[CT, CV]
) extends
    From[RNS, CT, CV]
    with from.JoinImpl[RNS, CT, CV]
object Join extends from.OJoinImpl

sealed abstract class AtomicFrom[+RNS, +CT, +CV] extends From[RNS, CT, CV] with from.AtomicFromImpl[RNS, CT, CV]
object AtomicFrom extends from.OAtomicFromImpl

object FromTable extends from.OFromTableImpl
case class FromTable[+RNS, +CT](
  tableName: DatabaseTableName,
  alias: Option[(RNS, ResourceName)],
  label: AutoTableLabel,
  columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]
) extends AtomicFrom[RNS, CT, Nothing] with from.FromTableImpl[RNS, CT]

// "alias" is optional here because of chained soql; actually having a
// real subselect syntactically requires an alias, but `select ... |>
// select ...` does not.  The alias is just for name-resolution during
// analysis anyway...
case class FromStatement[+RNS, +CT, +CV](
  statement: Statement[RNS, CT, CV],
  label: AutoTableLabel,
  alias: Option[(RNS, ResourceName)]
) extends AtomicFrom[RNS, CT, CV] with from.FromStatementImpl[RNS, CT, CV] {
  // I'm not sure why this needs to be here.  The typechecker gets
  // confused about calling Scope.apply if it lives in
  // FromStatementImpl
  private[analyzer2] val scope: Scope[CT] = Scope(statement.schema, label)
}
object FromStatement extends from.OFromStatementImpl

case class FromSingleRow[+RNS](
  label: AutoTableLabel,
  alias: Option[(RNS, ResourceName)]
) extends AtomicFrom[RNS, Nothing, Nothing] with from.FromSingleRowImpl[RNS]
object FromSingleRow extends from.OFromSingleRowImpl
