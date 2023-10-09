package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._

case class AugmentedType[MT <: MetaTypes with MetaTypesExt](rep: Rep[MT], isExpanded: Boolean) {
  def typ = rep.typ
}
