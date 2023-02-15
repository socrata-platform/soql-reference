package com.socrata.soql.analyzer2.from

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

import DocUtils._

trait FromStatementImpl[MT <: MetaTypes] { this: FromStatement[MT] =>
  type Self[MT <: MetaTypes] = FromStatement[MT]
  def asSelf = this

  private[analyzer2] val scope: Scope[MT] = new Scope.Virtual[MT](label, statement.schema)

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = statement.columnReferences

  def find(predicate: Expr[MT] => Boolean) = statement.find(predicate)
  def contains(e: Expr[MT]): Boolean =
    statement.contains(e)

  def unique = statement.unique.map(_.map { cn => VirtualColumn(label, cn, statement.schema(cn).typ)(AtomicPositionInfo.None) })

  private[analyzer2] final def findIsomorphism(state: IsomorphismState, that: From[MT]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromStatement(thatStatement, thatLabel, thatResourceName, thatAlias) =>
        state.tryAssociate(this.label, thatLabel) &&
          this.statement.findIsomorphism(state, Some(this.label), Some(thatLabel), thatStatement)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy(
      statement = statement.doRewriteDatabaseNames(state),
      resourceName = resourceName.map(state.changesOnlyLabels.convertRNSOnly(_))
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(statement = statement.doRelabel(state),
         label = state.convert(label))
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(statement = statement.mapAlias(f), alias = f(alias))

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] =
    statement.doAllTables(set)

  private[analyzer2] def realTables = statement.realTables

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    statement.doLabelMap(state)
    val tr = LabelMap.TableReference(resourceName, alias)
    state.tableMap += label -> tr
    for((columnLabel, NameEntry(columnName, _typ)) <- statement.schema) {
      state.columnMap += (label, columnLabel) -> (tr, columnName)
    }
  }

  private[analyzer2] def doDebugDoc(implicit ev: StatementDocProvider[MT]) =
    (statement.doDebugDoc.encloseNesting(d"(", d")") +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition[MT](alias, label))).annotate(Annotation.TableDefinition[MT](label))
}

trait OFromStatementImpl { this: FromStatement.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[FromStatement[MT]] = new Writable[FromStatement[MT]] {
    def writeTo(buffer: WriteBuffer, from: FromStatement[MT]): Unit = {
      buffer.write(from.statement)
      buffer.write(from.label)
      buffer.write(from.resourceName)
      buffer.write(from.alias)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[FromStatement[MT]] =
    new Readable[FromStatement[MT]] with MetaTypeHelper[MT] {
      def readFrom(buffer: ReadBuffer): FromStatement[MT] =
        FromStatement(
          statement = buffer.read[Statement[MT]](),
          label = buffer.read[AutoTableLabel](),
          resourceName = buffer.read[Option[ScopedResourceName[RNS]]](),
          alias = buffer.read[Option[ResourceName]]()
        )
    }
}
