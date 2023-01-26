package com.socrata.soql.analyzer2

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

  final type CT = ColumnType
  final type CV = ColumnValue
  final type RNS = ResourceNameScope
}

trait MetaTypeHelper[MT <: MetaTypes] {
  type CT = MT#ColumnType
  type CV = MT#ColumnValue
  type RNS = MT#ResourceNameScope

  import com.socrata.soql.analyzer2
}

trait SoQLAnalyzerUniverse[MT <: MetaTypes] extends MetaTypeHelper[MT] {
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
}

trait LabelHelper[MT <: MetaTypes] {
  import com.socrata.soql.analyzer2

  type TableLabel = analyzer2.TableLabel[MT#DatabaseTableNameImpl]
  type AutoTableLabel = analyzer2.AutoTableLabel
  type DatabaseTableName = analyzer2.DatabaseTableName[MT#DatabaseTableNameImpl]

  private[analyzer2] type IsomorphismState = analyzer2.IsomorphismState[MT]
  private[analyzer2] type RewriteDatabaseNamesState = analyzer2.RewriteDatabaseNamesState[MT]
}

trait SoQLAnalyzerExpressions[MT <: MetaTypes] extends MetaTypeHelper[MT] {
  import com.socrata.soql.analyzer2

  type Expr = analyzer2.Expr[MT]
  type AtomicExpr = analyzer2.AtomicExpr[MT]
  type Column = analyzer2.Column[MT]
  type SelectListReference = analyzer2.SelectListReference[MT]
  type Literal = analyzer2.Literal[MT]
  type LiteralValue = analyzer2.LiteralValue[MT]
  type NullLiteral = analyzer2.NullLiteral[MT]
  type FuncallLike = analyzer2.FuncallLike[MT]
  type FunctionCall = analyzer2.FunctionCall[MT]
  type AggregateFunctionCall = analyzer2.AggregateFunctionCall[MT]
  type WindowedFunctionCall = analyzer2.WindowedFunctionCall[MT]
}
