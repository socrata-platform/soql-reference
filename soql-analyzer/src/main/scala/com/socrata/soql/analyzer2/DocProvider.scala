package com.socrata.soql.analyzer2

private[analyzer2] class ExprDocProvider[MT <: MetaTypes](
  val cv: HasDoc[MT#ColumnValue],
  val columnNameImpl: HasDoc[MT#DatabaseColumnNameImpl]
)

private[analyzer2] class StatementDocProvider[MT <: MetaTypes](
  cv: HasDoc[MT#ColumnValue],
  val tableNameImpl: HasDoc[MT#DatabaseTableNameImpl],
  columnNameImpl: HasDoc[MT#DatabaseColumnNameImpl]
) extends ExprDocProvider(cv, columnNameImpl)
