package com.socrata.soql.environment

import com.ibm.icu.util.ULocale
import com.ibm.icu.text.Collator
import com.socrata.soql.collection.{OrderedMap, OrderedSet}

trait SchemalessDatasetContext {
  protected implicit def selfContext = this
  val locale: ULocale
  lazy val collator = Collator.getInstance(locale)
}

trait UntypedDatasetContext extends SchemalessDatasetContext {
  override protected implicit def selfContext = this
  val columns: OrderedSet[ColumnName] // Note: contains ALL columns, system AND user!
}

trait DatasetContext[Type] extends UntypedDatasetContext {
  override protected implicit def selfContext = this
  val schema: OrderedMap[ColumnName, Type] // Note: contains ALL columns, system AND user!
  lazy val columns: OrderedSet[ColumnName] = schema.keySet
}
