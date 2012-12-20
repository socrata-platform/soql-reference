package com.socrata.soql.typed

case class OrderBy[Type](expression: CoreExpr[Type], ascending: Boolean, nullLast: Boolean)
