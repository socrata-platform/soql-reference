package com.socrata.soql.typed

sealed trait Distinctiveness[ColumnId, Type] {
  def mapExpressions(f: CoreExpr[ColumnId, Type] => CoreExpr[ColumnId, Type]): Distinctiveness[ColumnId, Type]
}

case class Indistinct[ColumnId, Type]() extends Distinctiveness[ColumnId, Type] {
  def mapExpressions(f: CoreExpr[ColumnId, Type] => CoreExpr[ColumnId, Type]): this.type = this
}
case class FullyDistinct[ColumnId, Type]() extends Distinctiveness[ColumnId, Type] {
  def mapExpressions(f: CoreExpr[ColumnId, Type] => CoreExpr[ColumnId, Type]): this.type = this
}
case class DistinctOn[ColumnId, Type](exprs: Seq[CoreExpr[ColumnId, Type]]) extends Distinctiveness[ColumnId, Type] {
  def mapExpressions(f: CoreExpr[ColumnId, Type] => CoreExpr[ColumnId, Type]): DistinctOn[ColumnId, Type] =
    DistinctOn(exprs.map(f))
}
