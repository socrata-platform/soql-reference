package com.socrata.soql.analyzer2.statement

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

import DocUtils._

trait CTEImpl[MT <: MetaTypes] { this: CTE[MT] =>
  type Self[MT <: MetaTypes] = CTE[MT]
  def asSelf = this

  val schema = useQuery.schema

  def unique = useQuery.unique

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    definitionQuery.find(predicate).orElse(useQuery.find(predicate))

  def contains(e: Expr[MT]): Boolean =
    definitionQuery.contains(e) || useQuery.contains(e)

  private[analyzer2] def realTables =
    definitionQuery.realTables ++ useQuery.realTables

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    definitionQuery.columnReferences.mergeWith(useQuery.columnReferences)(_ ++ _)

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy(
      definitionQuery = definitionQuery.doRewriteDatabaseNames(state),
      useQuery = useQuery.doRewriteDatabaseNames(state)
    )

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT] =
    copy(definitionLabel = state.convert(definitionLabel),
         definitionQuery = definitionQuery.doRelabel(state),
         useQuery = useQuery.doRelabel(state))

  private[analyzer2] def findIsomorphism(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT]
  ): Boolean =
    that match {
      case CTE(thatDefLabel, _thatDefAlias, thatDefQuery, thatMatrHint, thatUseQuery) =>
        state.tryAssociate(this.definitionLabel, thatDefLabel) &&
          this.definitionQuery.findIsomorphism(state, Some(this.definitionLabel), Some(thatDefLabel), thatDefQuery) &&
          this.materializedHint == thatMatrHint &&
          this.useQuery.findIsomorphism(state, thisCurrentTableLabel, thatCurrentTableLabel, thatUseQuery)
      case _ =>
        false
    }

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(definitionQuery = definitionQuery.mapAlias(f), definitionAlias = f(definitionAlias), useQuery = useQuery.mapAlias(f))

  private[analyzer2] override def doDebugDoc(implicit ev: StatementDocProvider[MT]): Doc[Annotation[MT]] =
    Seq(
      Seq(
        Some(d"WITH" +#+ definitionLabel.debugDoc +#+ d"AS"),
        materializedHint.debugDoc
      ).flatten.hsep,
      definitionQuery.doDebugDoc.encloseNesting(d"(", d")"),
      useQuery.doDebugDoc
    ).sep

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    definitionQuery.doLabelMap(state)
    val tr = LabelMap.TableReference(None, None)
    state.tableMap += definitionLabel -> tr
    for((columnLabel, NameEntry(name, _typ)) <- definitionQuery.schema) {
      state.columnMap += (definitionLabel, columnLabel) -> (tr, name)
    }
    useQuery.doLabelMap(state)
  }
}

trait OCTEImpl { this: CTE.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[CTE[MT]] =
    new Writable[CTE[MT]] {
      def writeTo(buffer: WriteBuffer, ct: CTE[MT]): Unit = {
        buffer.write(ct.definitionLabel)
        buffer.write(ct.definitionAlias)
        buffer.write(ct.definitionQuery)
        buffer.write(ct.materializedHint)
        buffer.write(ct.useQuery)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[CTE[MT]] =
    new Readable[CTE[MT]] {
      def readFrom(buffer: ReadBuffer): CTE[MT] = {
        CTE(
          definitionLabel = buffer.read[AutoTableLabel](),
          definitionAlias = buffer.read[Option[ResourceName]](),
          definitionQuery = buffer.read[Statement[MT]](),
          materializedHint = buffer.read[MaterializedHint](),
          useQuery = buffer.read[Statement[MT]]()
        )
      }
    }
}
