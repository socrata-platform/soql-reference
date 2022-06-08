package com.socrata.soql.typed

case class OrderBy[ColumnId, Type](expression: CoreExpr[ColumnId, Type], ascending: Boolean, nullLast: Boolean) {
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId) = copy(expression = expression.mapColumnIds(f))
  def mapExpressions(f: CoreExpr[ColumnId, Type] => CoreExpr[ColumnId, Type]) = OrderBy(f(expression), ascending, nullLast)
}
