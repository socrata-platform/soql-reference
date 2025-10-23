package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

trait FromSingleRowImpl[MT <: MetaTypes] { this: FromSingleRow[MT] =>
  type Self[MT <: MetaTypes] = FromSingleRow[MT]
  def asSelf = this

  val resourceName = None

  // We have a unique ordering and no columns are required to achieve it
  def unique = LazyList(Nil)

  def schema = Nil

  def referencedCTEs = Set.empty[AutoCTELabel]

  private[analyzer2] val scope: Scope[MT] =
    new Scope.Virtual(label, OrderedMap.empty)

  def find(predicate: Expr[MT] => Boolean) = None
  def contains(e: Expr[MT]): Boolean = false

  private[analyzer2] final def findIsomorphismish(
    state: IsomorphismState,
    that: From[MT],
    recurseStmt: (Statement[MT], IsomorphismState, Option[AutoTableLabel], Option[AutoTableLabel], Statement[MT]) => Boolean
  ): Boolean =
    that match {
      case FromSingleRow(thatLabel, thatAlias) =>
        state.tryAssociate(this.label, thatLabel)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] final def findVerticalSlice(state: IsomorphismState, that: From[MT]): Boolean =
    findIsomorphism(state, that) // SingleRow has no columns, so this is the same as finding an isomorphism

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    this.asInstanceOf[FromSingleRow[MT2]] // safety: this has no labels

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT] = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromSingleRow[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(alias = f(alias))

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] = set

  private[analyzer2] def realTables = Map.empty[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    state.tableMap += label -> LabelMap.TableReference(resourceName, alias)
    // no columns
  }

  private[analyzer2] def doDebugDoc(implicit ev: StatementDocProvider[MT]) =
    (d"(SELECT)" +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition[MT](alias, label))).annotate(Annotation.TableDefinition[MT](label))

  override def nonlocalColumnReferences = Map.empty[AutoTableLabel, Set[ColumnLabel]]
}

trait OFromSingleRowImpl { this: FromSingleRow.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWRitable: Writable[MT#ResourceNameScope]): Writable[FromSingleRow[MT]] =
    new Writable[FromSingleRow[MT]] {
      def writeTo(buffer: WriteBuffer, fsr: FromSingleRow[MT]): Unit = {
        buffer.write(fsr.label)
        buffer.write(fsr.alias)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope]): Readable[FromSingleRow[MT]] =
    new Readable[FromSingleRow[MT]] {
      def readFrom(buffer: ReadBuffer): FromSingleRow[MT] = {
        FromSingleRow(
          label = buffer.read[AutoTableLabel](),
          alias = buffer.read[Option[ResourceName]]()
        )
      }
    }
}
