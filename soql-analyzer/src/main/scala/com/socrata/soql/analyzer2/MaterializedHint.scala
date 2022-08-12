package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

sealed abstract class MaterializedHint(val debugDoc: Option[Doc[Nothing]])
object MaterializedHint {
  case object Default extends MaterializedHint(None)
  case object Materialized extends MaterializedHint(Some(d"MATERIALIZED"))
  case object NotMaterialized extends MaterializedHint(Some(d"NOT MATERIALIZED"))
}
