package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer

trait MetaTypesExt { this: MetaTypes =>
  type ExtraContext <: sqlizer.ExtraContext[ExtraContextResult]
  type ExtraContextResult
  type CustomSqlizeAnnotation
  type SqlizerError
}
