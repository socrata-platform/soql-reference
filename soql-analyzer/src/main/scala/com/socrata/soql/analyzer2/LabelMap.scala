package com.socrata.soql.analyzer2

import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.OrJNull.implicits._

import com.socrata.soql.environment.{ColumnName, ResourceName}

class LabelMap[+RNS] private[analyzer2] (
  val tableMap: Map[TableLabel, Option[(RNS, ResourceName)]],
  val columnMap: Map[(TableLabel, ColumnLabel), (Option[(RNS, ResourceName)], ColumnName)]
) {
  override def toString = s"LabelMap($tableMap, $columnMap)"
}

object LabelMap {
  implicit def encode[RNS: JsonEncode] = new JsonEncode[LabelMap[RNS]] {
    private def encodeAlias(alias: (RNS, ResourceName)) =
      json"""{ scope: ${alias._1}, name: ${alias._2} }"""

    def encode(x: LabelMap[RNS]) = {
      val tmSeq = x.tableMap.to(LazyList).map { case (label, alias) =>
        json"""{ label: $label, alias: ${alias.map(encodeAlias).orJNull} }"""
      }
      val cmSeq = x.columnMap.to(LazyList).map { case ((tLabel, cLabel), (qual, col)) =>
        json"""{ tableLabel: $tLabel, columnLabel: $cLabel, qualifier: ${qual.map(encodeAlias).orJNull}, column: $col }"""
      }
      json"""{ tableMap: $tmSeq, columnMap: $cmSeq }"""
    }
  }
}

private[analyzer2] class LabelMapState[RNS] {
  val tableMap = Map.newBuilder[TableLabel, Option[(RNS, ResourceName)]]
  val columnMap = Map.newBuilder[(TableLabel, ColumnLabel), (Option[(RNS, ResourceName)], ColumnName)]

  def build() = new LabelMap(tableMap.result(), columnMap.result())
}
