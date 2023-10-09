package com.socrata.soql.sqlizer

import scala.reflect.ClassTag

import java.sql.ResultSet

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.analyzer2._

class ResultExtractor[MT <: MetaTypes with MetaTypesExt](
  rawSchema: OrderedMap[types.ColumnLabel[MT], AugmentedType[MT]]
)(implicit tag: ClassTag[MT#ColumnValue]) extends SqlizerUniverse[MT] {
  private val extractors = rawSchema.valuesIterator.map { case AugmentedType(rep, isExpanded) =>
    rep.extractFrom(isExpanded)
  }.toArray

  val schema: OrderedMap[ColumnLabel, CT] =
    OrderedMap() ++ rawSchema.iterator.map { case (c, AugmentedType(rep, v)) => c -> rep.typ }

  def extractRow(rs: ResultSet): Array[CV] = {
    val result = new Array[CV](extractors.length)

    var i = 0
    var dbCol = 1
    while(i != result.length) {
      val (width, value) = extractors(i)(rs, dbCol)

      result(i) = value
      i += 1
      dbCol += width
    }

    result
  }

  def fetchSize =
    if(rawSchema.valuesIterator.exists(_.rep.isPotentiallyLarge)) {
      10
    } else {
      1000
    }
}
