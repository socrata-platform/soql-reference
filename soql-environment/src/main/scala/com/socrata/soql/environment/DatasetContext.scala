package com.socrata.soql.environment

import com.socrata.soql.collection.{OrderedMap, OrderedSet}

trait UntypedDatasetContext {
  protected implicit def selfContext = this
  val columns: OrderedSet[ColumnName] // Note: contains ALL columns, system AND user!
}

trait DatasetContext[Type] extends UntypedDatasetContext {
  override protected implicit def selfContext = this
  val schema: OrderedMap[ColumnName, Type] // Note: contains ALL columns, system AND user!
  lazy val columns: OrderedSet[ColumnName] = {
    val result = OrderedSet.newBuilder[ColumnName]
    result ++= schema.keys
    result.result()
  }
}

object DatasetContext {
  def empty[Type]: DatasetContext[Type] = new DatasetContext[Type] {
    val schema = OrderedMap.empty[ColumnName, Type]
  }
}
