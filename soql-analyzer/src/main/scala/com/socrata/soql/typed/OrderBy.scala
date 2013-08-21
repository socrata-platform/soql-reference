package com.socrata.soql.typed

case class OrderBy[ColumnId, Type](expression: CoreExpr[ColumnId, Type], ascending: Boolean, nullLast: Boolean) {
  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) = copy(expression = expression.mapColumnIds(f))
}
