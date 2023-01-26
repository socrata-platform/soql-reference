package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait FromTableImpl[MT <: MetaTypes] { this: FromTable[MT] =>
  type Self[MT <: MetaTypes] = FromTable[MT]
  def asSelf = this

  def find(predicate: Expr[MT] => Boolean) = None
  def contains(e: Expr[MT]): Boolean = false

  def unique = primaryKeys.to(LazyList).map(_.map { dcn => Column(label, dcn, columns(dcn).typ)(AtomicPositionInfo.None) })

  lazy val resourceName = Some(definiteResourceName)

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] override final val scope: Scope[MT] = Scope(columns, label)

  def debugDoc(implicit ev: HasDoc[CV]) =
    (tableName.debugDoc ++ Doc.softlineSep ++ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition[MT](alias, label))).annotate(Annotation.TableDefinition[MT](label))

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      tableName = state.convert(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => state.convert(this.tableName, n) -> ne }
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] final def findIsomorphism(state: IsomorphismState, that: From[MT]): Boolean =
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

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(alias = f(alias))

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    val tr = LabelMap.TableReference(resourceName, alias)
    state.tableMap += label -> tr
    for((columnLabel, NameEntry(columnName, _typ)) <- columns) {
      state.columnMap += (label, columnLabel) -> (tr, columnName)
    }
  }
}

trait OFromTableImpl { this: FromTable.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#RNS], ctWritable: Writable[MT#CT], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl]): Writable[FromTable[MT]] = new Writable[FromTable[MT]] {
    def writeTo(buffer: WriteBuffer, from: FromTable[MT]): Unit = {
      buffer.write(from.tableName)
      buffer.write(from.definiteResourceName)
      buffer.write(from.alias)
      buffer.write(from.label)
      buffer.write(from.columns)
      buffer.write(from.primaryKeys)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#RNS], ctReadable: Readable[MT#CT], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl]): Readable[FromTable[MT]] = new Readable[FromTable[MT]] with MetaTypeHelper[MT] with LabelHelper[MT] {
    def readFrom(buffer: ReadBuffer): FromTable[MT] = {
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
