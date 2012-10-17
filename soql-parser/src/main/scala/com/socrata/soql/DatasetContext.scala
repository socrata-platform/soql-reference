package com.socrata.soql

import com.ibm.icu.util.ULocale
import com.ibm.icu.text.Collator

import com.socrata.soql.names.ColumnName
import com.socrata.collection.OrderedSet

trait DatasetContext {
  def locale: ULocale
  lazy val collator = Collator.getInstance(locale)
  def columns: OrderedSet[ColumnName] // Note: contains ALL columns, system AND user!
}
