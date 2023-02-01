package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.analyzer2

import DocUtils._

sealed abstract class From[MT <: MetaTypes] extends LabelUniverse[MT] {
  type Self[MT <: MetaTypes] <: From[MT]
  def asSelf: Self[MT]

  // A dataset is allowed to have zero or more sets of columns which
  // each uniquely specify rows (e.g., a UNION operation will have an
  // empty seq here, @single_row will have a single empty list
  // (because no columns are necessary to specify a unique ordering of
  // that special table), datasets will have one (:id) or two (:id and
  // the user-defined PK) groups, joins will have arbtrarily many.
  // This is Seq of Seq to leave the door open to multi-column PKs,
  // which is not currently a SoQL concept but why paint ourselves
  // into a corner?
  def unique: LazyList[Seq[Column[MT]]]

  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment(base: Environment[MT]): Either[AddScopeError, Environment[MT]]

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]): Self[MT2]

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT]

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: From[MT]): Boolean
  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]]

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]]
  def contains(e: Expr[MT]): Boolean

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder = debugDoc.layoutSmart().toStringBuilder(sb)
  def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[MT]]

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT]

  type ReduceResult[MT <: MetaTypes] <: From[MT]

  def reduceMap[S, MT2 <: MetaTypes](
    base: AtomicFrom[MT] => (S, AtomicFrom[MT2]),
    combine: (S, JoinType, Boolean, From[MT2], AtomicFrom[MT], Expr[MT]) => (S, Join[MT2])
  ): (S, ReduceResult[MT2])

  final def map[MT2 <: MetaTypes](
    base: AtomicFrom[MT] => AtomicFrom[MT2],
    combine: (JoinType, Boolean, From[MT2], AtomicFrom[MT], Expr[MT]) => Join[MT2]
  ): ReduceResult[MT2] = {
    reduceMap[Unit, MT2](
      { nonJoin => ((), base.apply(nonJoin)) },
      { (_, joinType, lateral, left, right, on) => ((), combine(joinType, lateral, left, right, on)) }
    )._2
  }

  final def reduce[S](
    base: AtomicFrom[MT] => S,
    combine: (S, Join[MT]) => S
  ): S =
    reduceMap[S, MT](
      { nonJoin => (base(nonJoin), nonJoin) },
      { (s, joinType, lateral, left, right, on) =>
        val j = Join(joinType, lateral, left, right, on)
        (combine(s, j), j)
      }
    )._1
}

object From {
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], ev: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[From[MT]] = new Writable[From[MT]] {
    def writeTo(buffer: WriteBuffer, from: From[MT]): Unit = {
      from match {
        case j: Join[MT] =>
          buffer.write(0)
          buffer.write(j)
        case ft: FromTable[MT] =>
          buffer.write(1)
          buffer.write(ft)
        case fs: FromStatement[MT] =>
          buffer.write(2)
          buffer.write(fs)
        case fsr: FromSingleRow[MT] =>
          buffer.write(3)
          buffer.write(fsr)
      }
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], ev: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[From[MT]] = new Readable[From[MT]] {
    def readFrom(buffer: ReadBuffer): From[MT] = {
      buffer.read[Int]() match {
        case 0 => buffer.read[Join[MT]]()
        case 1 => buffer.read[FromTable[MT]]()
        case 2 => buffer.read[FromStatement[MT]]()
        case 3 => buffer.read[FromSingleRow[MT]]()
        case other => fail("Unknown from tag " + other)
      }
    }
  }
}

case class Join[MT <: MetaTypes](
  joinType: JoinType,
  lateral: Boolean,
  left: From[MT],
  right: AtomicFrom[MT],
  on: Expr[MT]
) extends
    From[MT]
    with from.JoinImpl[MT]
object Join extends from.OJoinImpl

sealed abstract class AtomicFrom[MT <: MetaTypes] extends From[MT] with from.AtomicFromImpl[MT]
object AtomicFrom extends from.OAtomicFromImpl

case class FromTable[MT <: MetaTypes](
  tableName: DatabaseTableName[MT#DatabaseTableNameImpl],
  definiteResourceName: ScopedResourceName[MT#ResourceNameScope],
  alias: Option[ResourceName],
  label: AutoTableLabel,
  columns: OrderedMap[DatabaseColumnName[MT#DatabaseColumnNameImpl], NameEntry[MT#ColumnType]],
  primaryKeys: Seq[Seq[DatabaseColumnName[MT#DatabaseColumnNameImpl]]]
) extends AtomicFrom[MT] with from.FromTableImpl[MT]
object FromTable extends from.OFromTableImpl

// "alias" is optional here because of chained soql; actually having a
// real subselect syntactically requires an alias, but `select ... |>
// select ...` does not.  The alias is just for name-resolution during
// analysis anyway...
case class FromStatement[MT <: MetaTypes](
  statement: Statement[MT],
  label: AutoTableLabel,
  resourceName: Option[ScopedResourceName[MT#ResourceNameScope]],
  alias: Option[ResourceName]
) extends AtomicFrom[MT] with from.FromStatementImpl[MT]
object FromStatement extends from.OFromStatementImpl

case class FromSingleRow[MT <: MetaTypes](
  label: AutoTableLabel,
  alias: Option[ResourceName]
) extends AtomicFrom[MT] with from.FromSingleRowImpl[MT]
object FromSingleRow extends from.OFromSingleRowImpl
