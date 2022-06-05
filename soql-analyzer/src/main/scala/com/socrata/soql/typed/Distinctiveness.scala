package com.socrata.soql.typed

sealed trait Distinctiveness[ColumnId, Type]

case class Indistinct[ColumnId, Type]() extends Distinctiveness[ColumnId, Type]
case class FullyDistinct[ColumnId, Type]() extends Distinctiveness[ColumnId, Type]
case class DistinctOn[ColumnId, Type](exprs: Seq[CoreExpr[ColumnId, Type]]) extends Distinctiveness[ColumnId, Type]
