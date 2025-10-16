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

trait FromCTEImpl[MT <: MetaTypes] { this: FromCTE[MT] =>
  type Self[MT <: MetaTypes] = FromCTE[MT]
  def asSelf = this

  def find(predicate: Expr[MT] => Boolean) = None
  def contains(e: Expr[MT]): Boolean = false

  lazy val resourceName = Some(definiteResourceName)

  lazy val schema = statementSchema.iterator.map { case (columnLabel, ent) =>
    From.SchemaEntry(
      label, columnLabel, ent.typ, ent.hint,
      ent.isSynthetic
    )
  }.toVector

  // The schema of this CTE _as if_ it were still a FromStatment
  // (i.e., with the CTE's column labels)
  lazy val statementSchema =
    OrderedMap() ++ basedOn.schema.iterator.map { case (columnLabel, ent) =>
      columnMapping(columnLabel) -> ent
    }

  def unique = basedOn.unique.map(_.map { cn => VirtualColumn[MT](label, cn, basedOn.schema(cn).typ)(AtomicPositionInfo.Synthetic) })

  // A CTE does not, itself, contain any column references
  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = Map.empty

  private[analyzer2] override final val scope: Scope[MT] =
    new Scope.Virtual[MT](label, statementSchema.withValuesMapped(_.asNameEntry))

  private[analyzer2] def doDebugDoc(implicit ev: StatementDocProvider[MT]) =
    (cteLabel.debugDoc ++ Doc.softlineSep ++ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition[MT](alias,label))).annotate(Annotation.TableAliasDefinition[MT](alias, label))

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy[MT2](
      basedOn = basedOn.doRewriteDatabaseNames(state),
      definiteResourceName = state.changesOnlyLabels.convertRNSOnly(definiteResourceName)
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(
      basedOn = basedOn.doRelabel(state),
      cteLabel = state.convert(cteLabel),
      label = state.convert(label),
      columnMapping = columnMapping.iterator.map { case (a, b) => state.convert(a) -> state.convert(b) }.toMap
    )
  }

  private[analyzer2] final def findIsomorphismish(
    state: IsomorphismState,
    that: From[MT],
    recurseStmt: (Statement[MT], IsomorphismState, Option[AutoTableLabel], Option[AutoTableLabel], Statement[MT]) => Boolean
  ): Boolean =
    that match {
      case FromCTE(thatCteLabel, thatLabel, thatBasedOn, thatColumnMapping, thatResourceName, thatCanonicalName, thatAlias) =>
        state.tryAssociate(this.cteLabel, thatCteLabel) &&
          state.tryAssociate(this.label, thatLabel) &&
          // I think this is necessary, because sometimes -
          // particularly in rollup-rewrites - we're doing this in a
          // context where we won't "see" CTEs, so we need to use the
          // based-on to determine that we're referring to the same
          // CTE.
          // Actually, is this correct, or should this repaint the
          // "basedOn" Statements so their output column labels line
          // up with the froms'?
          recurseStmt(this.basedOn, state, Some(this.label), Some(thatLabel), thatBasedOn)
      case _ =>
        false
    }

  private[analyzer2] final def findVerticalSlice(state: IsomorphismState, that: From[MT]): Boolean =
    that match {
      case FromCTE(thatCteLabel, thatLabel, thatBasedOn, thatColumnMapping, thatResourceName, thatCanonicalName, thatAlias) =>
        state.tryAssociate(this.label, thatLabel) &&
          state.tryAssociate(this.cteLabel, thatCteLabel) &&
          // Actually, is this correct, or should this repaint the
          // "basedOn" Statements so their output column labels line
          // up with the froms'?
          this.basedOn.findVerticalSlice(state, Some(this.label), Some(thatLabel), thatBasedOn)
      case _ =>
        false
    }

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] = basedOn.doAllTables(set)
  private[analyzer2] def realTables = basedOn.realTables

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[MT] =
    copy(alias = newAlias)

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(alias = f(alias))

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    basedOn.doLabelMap(state)
    val tr = LabelMap.TableReference(resourceName, alias)
    state.tableMap += label -> tr
    for((columnLabel, Statement.SchemaEntry(columnName, _typ, _isSynthetic, _hint)) <- basedOn.schema) {
      state.columnMap += (label, columnMapping(columnLabel)) -> (tr, columnName)
    }
  }

  override def nonlocalColumnReferences = Map.empty[AutoTableLabel, Set[ColumnLabel]]
}

trait OFromCTEImpl { this: FromCTE.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[FromCTE[MT]] = new Writable[FromCTE[MT]] {
    def writeTo(buffer: WriteBuffer, from: FromCTE[MT]): Unit = {
      buffer.write(from.cteLabel)
      buffer.write(from.label)
      buffer.write(from.basedOn)
      buffer.write(from.columnMapping)
      buffer.write(from.definiteResourceName)
      buffer.write(from.canonicalName)
      buffer.write(from.alias)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[FromCTE[MT]] = new Readable[FromCTE[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): FromCTE[MT] = {
      FromCTE(
        cteLabel = buffer.read[AutoTableLabel](),
        label = buffer.read[AutoTableLabel](),
        basedOn = buffer.read[Statement[MT]](),
        columnMapping = buffer.read[Map[AutoColumnLabel, AutoColumnLabel]](),
        definiteResourceName = buffer.read[ScopedResourceName](),
        canonicalName = buffer.read[CanonicalName](),
        alias = buffer.read[Option[ResourceName]]()
      )
    }
  }
}
