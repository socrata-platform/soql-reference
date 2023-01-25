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
