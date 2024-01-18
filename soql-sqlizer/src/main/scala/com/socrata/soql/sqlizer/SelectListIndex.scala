package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._

case class SelectListIndex[MT <: MetaTypes with MetaTypesExt](startingPhysicalColumn: Int, exprSql: ExprSql[MT])
