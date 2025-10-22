package com.socrata.soql.analyzer2

import scala.language.higherKinds

import com.socrata.soql.functions
import com.socrata.soql.environment

trait MetaTypes {
  type ColumnType
  type ColumnValue

  /** The way in which saved queries are scoped.  This is nearly opaque
    * as far as TableFinder is concerned, requiring only that a tuple
    * of it and ResourceName make a valid hash table key.  Looking up
    * a name will include the scope in which further transitively
    * referenced names can be looked up.
    *
    * It can just be "()" if we have a flat namespace, or for example a
    * domain + user for federation...
    */
  type ResourceNameScope

  type DatabaseTableNameImpl
  type DatabaseColumnNameImpl
}

object MetaTypes {
  trait ChangesOnlyRNS[MT1 <: MetaTypes, MT2 <: MetaTypes] {
    def convertCT(t: MT1#ColumnType): MT2#ColumnType
    def convertCV(v: MT1#ColumnValue): MT2#ColumnValue
    def convertDTN(dtn: DatabaseTableName[MT1#DatabaseTableNameImpl]): DatabaseTableName[MT2#DatabaseTableNameImpl]
    def convertDCN(dcn: DatabaseColumnName[MT1#DatabaseColumnNameImpl]): DatabaseColumnName[MT2#DatabaseColumnNameImpl]

    def convertCTOnly[F[_]](f: F[MT1#ColumnType]): F[MT2#ColumnType] =
      f.asInstanceOf[F[MT2#ColumnType]]
  }

  object ChangesOnlyRNS {
    implicit def evidence[MT1 <: MetaTypes, MT2 <: MetaTypes](
      implicit ev1: MT1#ColumnType =:= MT2#ColumnType,
      ev2: MT1#ColumnValue =:= MT2#ColumnValue,
      ev3: DatabaseTableName[MT1#DatabaseTableNameImpl] =:= DatabaseTableName[MT2#DatabaseTableNameImpl],
      ev4: DatabaseColumnName[MT1#DatabaseColumnNameImpl] =:= DatabaseColumnName[MT2#DatabaseColumnNameImpl]
    ): ChangesOnlyRNS[MT1, MT2] =
      new ChangesOnlyRNS[MT1, MT2] {
        def convertCT(t: MT1#ColumnType): MT2#ColumnType = t
        def convertCV(v: MT1#ColumnValue): MT2#ColumnValue = v
        def convertDTN(dtn: DatabaseTableName[MT1#DatabaseTableNameImpl]): DatabaseTableName[MT2#DatabaseTableNameImpl] = dtn
        def convertDCN(dcn: DatabaseColumnName[MT1#DatabaseColumnNameImpl]): DatabaseColumnName[MT2#DatabaseColumnNameImpl] = dcn
      }
  }

  trait ChangesOnlyLabels[MT1 <: MetaTypes, MT2 <: MetaTypes] {
    def convertCT(t: MT1#ColumnType): MT2#ColumnType
    def convertCV(v: MT1#ColumnValue): MT2#ColumnValue
    def convertRNS(rns: MT1#ResourceNameScope): MT2#ResourceNameScope

    def convertCTOnly[F[_]](f: F[MT1#ColumnType]): F[MT2#ColumnType] =
      f.asInstanceOf[F[MT2#ColumnType]]

    def convertRNSOnly[F[_]](f: F[MT1#ResourceNameScope]): F[MT2#ResourceNameScope] =
      f.asInstanceOf[F[MT2#ResourceNameScope]]
  }

  object ChangesOnlyLabels {
    implicit def evidence[MT1 <: MetaTypes, MT2 <: MetaTypes](
      implicit ev1: MT1#ColumnType =:= MT2#ColumnType,
      ev2: MT1#ColumnValue =:= MT2#ColumnValue,
      ev3: MT1#ResourceNameScope =:= MT2#ResourceNameScope
    ): ChangesOnlyLabels[MT1, MT2] =
      new ChangesOnlyLabels[MT1, MT2] {
        def convertCT(t: MT1#ColumnType): MT2#ColumnType = t
        def convertCV(v: MT1#ColumnValue): MT2#ColumnValue = v
        def convertRNS(rns: MT1#ResourceNameScope): MT2#ResourceNameScope = rns
      }
  }
}

trait MetaTypeHelper[MT <: MetaTypes] {
  type CT = MT#ColumnType
  type CV = MT#ColumnValue
  type RNS = MT#ResourceNameScope

  import com.socrata.soql.analyzer2
}

trait LabelUniverse[MT <: MetaTypes] extends MetaTypeHelper[MT] {
  import com.socrata.soql.analyzer2

  type AutoTableLabel = analyzer2.AutoTableLabel
  type DatabaseTableName = analyzer2.DatabaseTableName[MT#DatabaseTableNameImpl]

  type ColumnLabel = analyzer2.ColumnLabel[MT#DatabaseColumnNameImpl]
  type AutoColumnLabel = analyzer2.AutoColumnLabel
  type DatabaseColumnName = analyzer2.DatabaseColumnName[MT#DatabaseColumnNameImpl]

  type ScopedResourceName = environment.ScopedResourceName[MT#ResourceNameScope]
  type Source = environment.Source[MT#ResourceNameScope]
  type ToProvenance = analyzer2.ToProvenance[MT#DatabaseTableNameImpl]
  type FromProvenance = analyzer2.FromProvenance[MT#DatabaseTableNameImpl]
  type ProvenanceMapper = analyzer2.ProvenanceMapper[MT#DatabaseTableNameImpl]

  type PositionInfo = analyzer2.PositionInfo[MT#ResourceNameScope]
  type AtomicPositionInfo = analyzer2.AtomicPositionInfo[MT#ResourceNameScope]
  type FuncallPositionInfo = analyzer2.FuncallPositionInfo[MT#ResourceNameScope]

  private[analyzer2] type IsomorphismState = analyzer2.IsomorphismState[MT]
  private[analyzer2] type RewriteDatabaseNamesState[MT2 <: MetaTypes] = analyzer2.RewriteDatabaseNamesState[MT, MT2]
}

trait ExpressionUniverse[MT <: MetaTypes] extends LabelUniverse[MT] {
  import com.socrata.soql.analyzer2

  type Expr = analyzer2.Expr[MT]
  type AtomicExpr = analyzer2.AtomicExpr[MT]
  type Column = analyzer2.Column[MT]
  type PhysicalColumn = analyzer2.PhysicalColumn[MT]
  type VirtualColumn = analyzer2.VirtualColumn[MT]
  type SelectListReference = analyzer2.SelectListReference[MT]
  type Literal = analyzer2.Literal[MT]
  type LiteralValue = analyzer2.LiteralValue[MT]
  type NullLiteral = analyzer2.NullLiteral[MT]
  type FuncallLike = analyzer2.FuncallLike[MT]
  type FunctionCall = analyzer2.FunctionCall[MT]
  type AggregateFunctionCall = analyzer2.AggregateFunctionCall[MT]
  type WindowedFunctionCall = analyzer2.WindowedFunctionCall[MT]

  type OrderBy = analyzer2.OrderBy[MT] // this is here because it can occur in windowed function calls
  type MonomorphicFunction = functions.MonomorphicFunction[CT]
}

trait StatementUniverse[MT <: MetaTypes] extends ExpressionUniverse[MT] with LabelUniverse[MT] {
  import com.socrata.soql.analyzer2

  type Statement = analyzer2.Statement[MT]
  type Select = analyzer2.Select[MT]
  type CTE = analyzer2.CTE[MT]
  type CombinedTables = analyzer2.CombinedTables[MT]
  type Values = analyzer2.Values[MT]

  type From = analyzer2.From[MT]
  type Join = analyzer2.Join[MT]
  type AtomicFrom = analyzer2.AtomicFrom[MT]
  type FromTable = analyzer2.FromTable[MT]
  type FromSingleRow = analyzer2.FromSingleRow[MT]
  type FromStatement = analyzer2.FromStatement[MT]
  type FromCTE = analyzer2.FromCTE[MT]

  type Distinctiveness = analyzer2.Distinctiveness[MT]
  type NamedExpr = analyzer2.NamedExpr[MT]

  type AvailableCTEs[T] = analyzer2.AvailableCTEs[MT, T]
}
