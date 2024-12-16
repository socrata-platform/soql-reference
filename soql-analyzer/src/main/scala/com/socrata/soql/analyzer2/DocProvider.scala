package com.socrata.soql.analyzer2

private[analyzer2] class StatementDocProvider[MT <: MetaTypes](
  val cv: HasDoc[MT#ColumnValue],
  val tableNameImpl: HasDoc[MT#DatabaseTableNameImpl],
  val columnNameImpl: HasDoc[MT#DatabaseColumnNameImpl]
)
