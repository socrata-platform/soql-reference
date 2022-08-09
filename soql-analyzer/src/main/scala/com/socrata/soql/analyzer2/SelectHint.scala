package com.socrata.soql.analyzer2

sealed trait SelectHint
object SelectHint {
  case object Materialized extends SelectHint
  case object NoRollup extends SelectHint
  case object NoChainMerge extends SelectHint
}
