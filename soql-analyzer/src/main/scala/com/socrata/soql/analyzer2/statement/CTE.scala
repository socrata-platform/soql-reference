package com.socrata.soql.analyzer2.statement

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

import DocUtils._

trait CTEImpl[+RNS, +CT, +CV] { this: CTE[RNS, CT, CV] =>
  type Self[+RNS, +CT, +CV] = CTE[RNS, CT, CV]
  def asSelf = this

  val schema = useQuery.schema

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    definitionQuery.find(predicate).orElse(useQuery.find(predicate))

  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    definitionQuery.contains(e) || useQuery.contains(e)

  private[analyzer2] def realTables =
    definitionQuery.realTables ++ useQuery.realTables

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    definitionQuery.columnReferences.mergeWith(useQuery.columnReferences)(_ ++ _)

  def useSelectListReferences = copy(definitionQuery = definitionQuery.useSelectListReferences, useQuery = useQuery.useSelectListReferences)
  def unuseSelectListReferences = copy(definitionQuery = definitionQuery.unuseSelectListReferences, useQuery = useQuery.unuseSelectListReferences)

  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]], myLabel: Option[TableLabel]): Self[RNS, CT, CV] =
    copy(
      definitionQuery = definitionQuery.doRemoveUnusedColumns(used, Some(definitionLabel)),
      useQuery = useQuery.doRemoveUnusedColumns(used, myLabel)
    )

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      definitionQuery = definitionQuery.doRewriteDatabaseNames(state),
      useQuery = useQuery.doRewriteDatabaseNames(state)
    )

  private[analyzer2] def doRelabel(state: RelabelState): Self[RNS, CT, CV] =
    copy(definitionLabel = state.convert(definitionLabel),
         definitionQuery = definitionQuery.doRelabel(state),
         useQuery = useQuery.doRelabel(state))

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[AutoColumnLabel], Self[RNS, CT2, CV]) = {
    val (orderingColumn, newUseQuery) = useQuery.preserveOrdering(provider, rowNumberFunction, wantOutputOrdered, wantOrderingColumn)
    (
      orderingColumn,
      copy(
        definitionQuery = definitionQuery.preserveOrdering(provider, rowNumberFunction, false, false)._2,
        useQuery = newUseQuery
      )
    )
  }

  private[analyzer2] def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](
    state: IsomorphismState,
    thisCurrentTableLabel: Option[TableLabel],
    thatCurrentTableLabel: Option[TableLabel],
    that: Statement[RNS2, CT2, CV2]
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

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[RNS, CT, CV] =
    copy(definitionQuery = definitionQuery.mapAlias(f), definitionAlias = f(definitionAlias), useQuery = useQuery.mapAlias(f))

  override def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[RNS, CT]] =
    Seq(
      Seq(
        Some(d"WITH" +#+ definitionLabel.debugDoc +#+ d"AS"),
        materializedHint.debugDoc
      ).flatten.hsep,
      definitionQuery.debugDoc.encloseNesting(d"(", d")"),
      useQuery.debugDoc
    ).sep

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
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
  implicit def serialize[RNS: Writable, CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]): Writable[CTE[RNS, CT, CV]] =
    new Writable[CTE[RNS, CT, CV]] {
      def writeTo(buffer: WriteBuffer, ct: CTE[RNS, CT, CV]): Unit = {
        buffer.write(ct.definitionLabel)
        buffer.write(ct.definitionAlias)
        buffer.write(ct.definitionQuery)
        buffer.write(ct.materializedHint)
        buffer.write(ct.useQuery)
      }
    }

  implicit def deserialize[RNS: Readable, CT: Readable, CV](implicit ev: Readable[Expr[CT, CV]]): Readable[CTE[RNS, CT, CV]] =
    new Readable[CTE[RNS, CT, CV]] {
      def readFrom(buffer: ReadBuffer): CTE[RNS, CT, CV] = {
        CTE(
          definitionLabel = buffer.read[AutoTableLabel](),
          definitionAlias = buffer.read[Option[ResourceName]](),
          definitionQuery = buffer.read[Statement[RNS, CT, CV]](),
          materializedHint = buffer.read[MaterializedHint](),
          useQuery = buffer.read[Statement[RNS, CT, CV]]()
        )
      }
    }
}
