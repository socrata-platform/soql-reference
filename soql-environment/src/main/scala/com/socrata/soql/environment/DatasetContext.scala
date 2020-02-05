package com.socrata.soql.environment

import com.socrata.soql.collection.{OrderedMap, OrderedSet}

trait DatasetContext[Type] {
  val schema: OrderedMap[ColumnName, Type] // Note: contains ALL columns, system AND user!
}
