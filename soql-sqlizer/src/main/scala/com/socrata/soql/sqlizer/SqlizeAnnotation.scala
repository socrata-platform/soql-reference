package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName

sealed abstract class SqlizeAnnotation[MT <: MetaTypes with MetaTypesExt] extends SqlizerUniverse[MT]

object SqlizeAnnotation {
  case class Expression[MT <: MetaTypes with MetaTypesExt](expr: Expr[MT]) extends SqlizeAnnotation[MT]
  case class Table[MT <: MetaTypes with MetaTypesExt](table: AutoTableLabel) extends SqlizeAnnotation[MT]
  case class OutputName[MT <: MetaTypes with MetaTypesExt](name: ColumnName) extends SqlizeAnnotation[MT]
  case class Custom[MT <: MetaTypes with MetaTypesExt](ann: MT#CustomSqlizeAnnotation) extends SqlizeAnnotation[MT]
}
