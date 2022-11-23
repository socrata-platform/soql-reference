package com.socrata.soql.analyzer2

import com.socrata.soql.environment.{ColumnName, ResourceName}

class LabelMap[+RNS] private[analyzer2] (
  val tableMap: Map[TableLabel, Option[(RNS, ResourceName)]],
  val columnMap: Map[(TableLabel, ColumnLabel), (Option[(RNS, ResourceName)], ColumnName)]
)

private[analyzer2] class LabelMapState[RNS] {
  val tableMap = Map.newBuilder[TableLabel, Option[(RNS, ResourceName)]]
  val columnMap = Map.newBuilder[(TableLabel, ColumnLabel), (Option[(RNS, ResourceName)], ColumnName)]

  def build() = new LabelMap(tableMap.result(), columnMap.result())
}
