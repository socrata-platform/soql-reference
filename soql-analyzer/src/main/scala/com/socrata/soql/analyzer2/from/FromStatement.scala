package com.socrata.soql.analyzer2.from

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

import DocUtils._

trait FromStatementImpl[MT <: MetaTypes] { this: FromStatement[MT] =>
  type Self[MT <: MetaTypes] = FromStatement[MT]
  def asSelf = this

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = statement.columnReferences

  def find(predicate: Expr[CT, CV] => Boolean) = statement.find(predicate)
  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    statement.contains(e)

  def unique = statement.unique.map(_.map { cn => Column(label, cn, statement.column(cn).typ)(AtomicPositionInfo.None) })

  private[analyzer2] final def findIsomorphism[MT2 <: MetaTypes](state: IsomorphismState, that: From[MT2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromStatement(thatStatement, thatLabel, thatResourceName, thatAlias) =>
        state.tryAssociate(this.label, thatLabel) &&
          this.statement.findIsomorphism(state, Some(this.label), Some(thatLabel), thatStatement)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(statement = statement.doRewriteDatabaseNames(state))

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(statement = statement.doRelabel(state),
         label = state.convert(label))
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(statement = statement.mapAlias(f), alias = f(alias))

  private[analyzer2] def realTables = statement.realTables

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
    statement.doLabelMap(state)
    val tr = LabelMap.TableReference(resourceName, alias)
    state.tableMap += label -> tr
    for((columnLabel, NameEntry(columnName, _typ)) <- statement.schema) {
      state.columnMap += (label, columnLabel) -> (tr, columnName)
    }
  }

  def debugDoc(implicit ev: HasDoc[CV]) =
    (statement.debugDoc.encloseNesting(d"(", d")") +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}

trait OFromStatementImpl { this: FromStatement.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#RNS], ctWritable: Writable[MT#CT], exprWritable: Writable[Expr[MT#CT, MT#CV]]): Writable[FromStatement[MT]] = new Writable[FromStatement[MT]] {
    def writeTo(buffer: WriteBuffer, from: FromStatement[MT]): Unit = {
      buffer.write(from.statement)
      buffer.write(from.label)
      buffer.write(from.resourceName)
      buffer.write(from.alias)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#RNS], ctReadable: Readable[MT#CT], exprReadable: Readable[Expr[MT#CT, MT#CV]]): Readable[FromStatement[MT]] =
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
