package com.socrata.soql.analyzer2

sealed abstract class MaterializedHint
object MaterializedHint {
  case object Default extends MaterializedHint
  case object Materialized extends MaterializedHint
  case object NotMaterialized extends MaterializedHint
}
