package com.socrata.soql.typed

case class OrderBy[Type](expression: TypedFF[Type], ascending: Boolean, nullLast: Boolean)
