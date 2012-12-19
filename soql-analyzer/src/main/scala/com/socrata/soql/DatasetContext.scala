package com.socrata.soql

import com.ibm.icu.util.ULocale
import com.ibm.icu.text.Collator

import com.socrata.soql.names.ColumnName
import com.socrata.collection.{OrderedMap, OrderedSet}

trait SchemalessDatasetContext {
  val locale: ULocale
  lazy val collator = Collator.getInstance(locale)
}

trait UntypedDatasetContext extends SchemalessDatasetContext {
  val columns: OrderedSet[ColumnName] // Note: contains ALL columns, system AND user!
}

trait DatasetContext[Type] extends UntypedDatasetContext {
  val schema: OrderedMap[ColumnName, Type] // Note: contains ALL columns, system AND user!
  lazy val columns: OrderedSet[ColumnName] = schema.keySet
}
