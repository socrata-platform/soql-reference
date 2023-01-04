package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait FromTableImpl[+RNS, +CT] { this: FromTable[RNS, CT] =>
  type Self[+RNS, +CT, +CV] = FromTable[RNS, CT]
  def asSelf = this

  def find(predicate: Expr[CT, Nothing] => Boolean) = None
  def contains[CT2 >: CT, CV](e: Expr[CT2, CV]): Boolean =
    false

  def unique = primaryKeys.to(LazyList).map(_.map { dcn => Column(label, dcn, columns(dcn).typ)(AtomicPositionInfo.None) })

  lazy val resourceName = Some(definiteResourceName)

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] override final val scope: Scope[CT] = Scope(columns, label)

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (tableName.debugDoc ++ Doc.softlineSep ++ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      tableName = state.convert(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => state.convert(this.tableName, n) -> ne }
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromTable(thatTableName, thatResourceName, thatAlias, thatLabel, thatColumns, thatPrimaryKeys) =>
        this.tableName == thatTableName &&
          // don't care about aliases
          state.tryAssociate(this.label, thatLabel) &&
          this.columns.size == thatColumns.size &&
          this.columns.iterator.zip(thatColumns.iterator).forall { case ((thisColName, thisEntry), (thatColName, thatEntry)) =>
            thisColName == thatColName &&
              thisEntry.typ == thatEntry.typ
            // don't care about the entry's name
          } &&
          this.primaryKeys == thatPrimaryKeys
      case _ =>
        false
    }

  private[analyzer2] def realTables = Map(label -> tableName)

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromTable[RNS, CT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[RNS, CT, Nothing] =
    copy(alias = f(alias))

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
    val tr = LabelMap.TableReference(resourceName, alias)
    state.tableMap += label -> tr
    for((columnLabel, NameEntry(columnName, _typ)) <- columns) {
      state.columnMap += (label, columnLabel) -> (tr, columnName)
    }
  }
}

trait OFromTableImpl { this: FromTable.type =>
  implicit def serialize[RNS: Writable, CT: Writable]: Writable[FromTable[RNS, CT]] = new Writable[FromTable[RNS, CT]] {
    def writeTo(buffer: WriteBuffer, from: FromTable[RNS, CT]): Unit = {
      buffer.write(from.tableName)
      buffer.write(from.definiteResourceName)
      buffer.write(from.alias)
      buffer.write(from.label)
      buffer.write(from.columns)
      buffer.write(from.primaryKeys)
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable]: Readable[FromTable[RNS, CT]] = new Readable[FromTable[RNS, CT]] {
    def readFrom(buffer: ReadBuffer): FromTable[RNS, CT] = {
      FromTable(
        tableName = buffer.read[DatabaseTableName](),
        definiteResourceName = buffer.read[ScopedResourceName[RNS]](),
        alias = buffer.read[Option[ResourceName]](),
        label = buffer.read[AutoTableLabel](),
        columns = buffer.read[OrderedMap[DatabaseColumnName, NameEntry[CT]]](),
        primaryKeys = buffer.read[Seq[Seq[DatabaseColumnName]]]()
      )
    }
  }
}
