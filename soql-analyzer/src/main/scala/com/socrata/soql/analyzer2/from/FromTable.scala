package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.ast.JValue

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer, Version}
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, ScopedResourceName, ColumnName}
import com.socrata.soql.functions.MonomorphicFunction

trait FromTableImpl[MT <: MetaTypes] { this: FromTable[MT] =>
  type Self[MT <: MetaTypes] = FromTable[MT]
  def asSelf = this

  def referencedCTEs = Set.empty[AutoTableLabel]

  def find(predicate: Expr[MT] => Boolean) = None
  def contains(e: Expr[MT]): Boolean = false

  def schema = columns.iterator.map { case (dcn, FromTable.ColumnInfo(_, typ, hint)) =>
    From.SchemaEntry(
      label, dcn, typ, hint,
      isSynthetic = false // table columns are never synthetic
    )
  }.toVector

  def unique = primaryKeys.to(LazyList).map(_.map { dcn => PhysicalColumn[MT](label, tableName, dcn, columns(dcn).typ)(AtomicPositionInfo.Synthetic) })

  lazy val resourceName = Some(definiteResourceName)

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] override final val scope: Scope[MT] = new Scope.Physical[MT](tableName, label, columns.withValuesMapped(_.asNameEntry))

  private[analyzer2] def doDebugDoc(implicit ev: StatementDocProvider[MT]) =
    (tableName.debugDoc(ev.tableNameImpl) ++ Doc.softlineSep ++ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition[MT](alias, label))).annotate(Annotation.TableDefinition[MT](label))

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy[MT2](
      tableName = state.convert(this.tableName),
      definiteResourceName = state.changesOnlyLabels.convertRNSOnly(definiteResourceName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, columnInfo) =>
        state.convert(this.tableName, n) -> columnInfo.doRewriteDatabaseNames(state.changesOnlyLabels)
      },
      primaryKeys = primaryKeys.map(_.map(state.convert(this.tableName, _)))
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] final def findIsomorphismish(
    state: IsomorphismState,
    that: From[MT],
    recurseStmt: (Statement[MT], IsomorphismState, Option[AutoTableLabel], Option[AutoTableLabel], Statement[MT]) => Boolean
  ): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromTable(thatTableName, thatResourceName, thatCanonicalName, thatAlias, thatLabel, thatColumns, thatPrimaryKeys) =>
        this.tableName == thatTableName &&
          // don't care about aliases
          state.tryAssociate(this.label, thatLabel) &&
          this.columns.size == thatColumns.size &&
          this.columns.iterator.zip(thatColumns.iterator).forall { case ((thisColName, thisEntry), (thatColName, thatEntry)) =>
            thisColName == thatColName &&
              thisEntry.typ == thatEntry.typ
            // don't care about the entry's name.  Or hints?
          } &&
          this.primaryKeys.toSet == thatPrimaryKeys.toSet
      case _ =>
        false
    }

  private[analyzer2] final def findVerticalSlice(state: IsomorphismState, that: From[MT]): Boolean =
    that match {
      case FromTable(thatTableName, thatResourceName, thatCanonicalName, thatAlias, thatLabel, thatColumns, thatPrimaryKeys) =>
        this.tableName == thatTableName &&
          state.tryAssociate(this.label, thatLabel) &&
          this.columns.forall { case (thisColName, thisEntry) =>
            // We care about column database names and types, but not
            // human names.
            thatColumns.get(thisColName).fold(false) { thatEntry =>
              thisEntry.typ == thatEntry.typ
            }
          } &&
          this.primaryKeys.toSet.subsetOf(thatPrimaryKeys.toSet)
      case _ =>
        false
    }

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] = set + tableName
  private[analyzer2] def realTables = Map(label -> tableName)

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(alias = f(alias))

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    val tr = LabelMap.TableReference(resourceName, alias)
    state.tableMap += label -> tr
    for((columnLabel, FromTable.ColumnInfo(columnName, _typ, _hint)) <- columns) {
      state.columnMap += (label, columnLabel) -> (tr, columnName)
    }
  }

  override def nonlocalColumnReferences = Map.empty[AutoTableLabel, Set[ColumnLabel]]
}

trait OFromTableImpl { this: FromTable.type =>
  case class ColumnInfo[MT <: MetaTypes](name: ColumnName, typ: types.ColumnType[MT], hint: Option[JValue]) {
    def asNameEntry = NameEntry(name, typ)

    private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](ev: MetaTypes.ChangesOnlyLabels[MT, MT2]) =
      ColumnInfo(name, ev.convertCT(typ), hint)
  }

  object ColumnInfo {
    implicit def serialize[MT <: MetaTypes](implicit ctWritable: Writable[types.ColumnType[MT]]): Writable[ColumnInfo[MT]] = new Writable[ColumnInfo[MT]] {
      def writeTo(buffer: WriteBuffer, from: ColumnInfo[MT]): Unit = {
        buffer.write(from.name)
        buffer.write(from.typ)
        buffer.write(from.hint)
      }
    }

    implicit def deserialize[MT <: MetaTypes](implicit ctReadable: Readable[types.ColumnType[MT]]): Readable[ColumnInfo[MT]] = new Readable[ColumnInfo[MT]] {
      def readFrom(buffer: ReadBuffer): ColumnInfo[MT] = {
        ColumnInfo[MT](
          name = buffer.read[ColumnName](),
          typ = buffer.read[types.ColumnType[MT]](),
          hint = buffer.read[Option[JValue]]()
        )
      }
    }
  }

  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[FromTable[MT]] = new Writable[FromTable[MT]] {
    def writeTo(buffer: WriteBuffer, from: FromTable[MT]): Unit = {
      buffer.write(from.tableName)
      buffer.write(from.definiteResourceName)
      buffer.write(from.definiteCanonicalName)
      buffer.write(from.alias)
      buffer.write(from.label)
      buffer.write(from.columns)
      buffer.write(from.primaryKeys)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[FromTable[MT]] = new Readable[FromTable[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): FromTable[MT] = {
      buffer.version match {
        case Version.V6 =>
          FromTable(
            tableName = buffer.read[DatabaseTableName](),
            definiteResourceName = buffer.read[ScopedResourceName](),
            definiteCanonicalName = CanonicalName("invalid - this is only (at time of migration) used by MaterializeNamedQueries, which will not happen in a V6 serialization"),
            alias = buffer.read[Option[ResourceName]](),
            label = buffer.read[AutoTableLabel](),
            columns = buffer.read[OrderedMap[DatabaseColumnName, ColumnInfo[MT]]](),
            primaryKeys = buffer.read[Seq[Seq[DatabaseColumnName]]]()
          )
        case Version.V7 =>
            FromTable(
              tableName = buffer.read[DatabaseTableName](),
              definiteResourceName = buffer.read[ScopedResourceName](),
              definiteCanonicalName = buffer.read[CanonicalName](),
              alias = buffer.read[Option[ResourceName]](),
              label = buffer.read[AutoTableLabel](),
              columns = buffer.read[OrderedMap[DatabaseColumnName, ColumnInfo[MT]]](),
              primaryKeys = buffer.read[Seq[Seq[DatabaseColumnName]]]()
            )
      }
    }
  }
}
