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

  final type CT = ColumnType
  final type CV = ColumnValue
  final type RNS = ResourceNameScope
}

trait MetaTypeHelper[MT <: MetaTypes] {
  type CT = MT#ColumnType
  type CV = MT#ColumnValue
  type RNS = MT#ResourceNameScope
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
