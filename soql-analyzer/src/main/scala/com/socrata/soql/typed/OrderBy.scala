package com.socrata.soql.typed

case class OrderBy[ColumnId, Type](expression: CoreExpr[ColumnId, Type], ascending: Boolean, nullLast: Boolean) {
  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) = copy(expression = expression.mapColumnIds(f))
  def mapAccumColumnIds[State, NewColumnId](s0: State)(f: (State, ColumnId) => (State, NewColumnId)) = {
    val (s1, newExpression) = expression.mapAccumColumnIds(s0)(f)
    (s1, copy(expression = newExpression))
  }
}
