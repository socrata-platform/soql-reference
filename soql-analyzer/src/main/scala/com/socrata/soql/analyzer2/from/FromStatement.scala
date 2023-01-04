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

trait FromStatementImpl[+RNS, +CT, +CV] { this: FromStatement[RNS, CT, CV] =>
  type Self[+RNS, +CT, +CV] = FromStatement[RNS, CT, CV]
  def asSelf = this

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = statement.columnReferences

  def find(predicate: Expr[CT, CV] => Boolean) = statement.find(predicate)
  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    statement.contains(e)

  def unique = statement.unique.map(_.map { cn => Column(label, cn, statement.column(cn).typ)(AtomicPositionInfo.None) })

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
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

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromStatement[RNS, CT, CV] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[RNS, CT, CV] =
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
  implicit def serialize[RNS: Writable, CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]): Writable[FromStatement[RNS, CT, CV]] = new Writable[FromStatement[RNS, CT, CV]] {
    def writeTo(buffer: WriteBuffer, from: FromStatement[RNS, CT, CV]): Unit = {
      buffer.write(from.statement)
      buffer.write(from.label)
      buffer.write(from.resourceName)
      buffer.write(from.alias)
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable, CV](implicit ev: Readable[Expr[CT, CV]]): Readable[FromStatement[RNS, CT, CV]] =
    new Readable[FromStatement[RNS, CT, CV]] {
      def readFrom(buffer: ReadBuffer): FromStatement[RNS, CT, CV] =
        FromStatement(
          statement = buffer.read[Statement[RNS, CT, CV]](),
          label = buffer.read[AutoTableLabel](),
          resourceName = buffer.read[Option[ScopedResourceName[RNS]]](),
          alias = buffer.read[Option[ResourceName]]()
        )
    }
}
