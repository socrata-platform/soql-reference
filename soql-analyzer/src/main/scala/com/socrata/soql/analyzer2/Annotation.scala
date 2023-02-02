package com.socrata.soql.analyzer2

import com.socrata.prettyprint.{tree, SimpleDocTree}
import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil

import com.socrata.soql.environment.{ResourceName, ColumnName}

sealed abstract class Annotation[MT <: MetaTypes]
object Annotation {
  case class TableAliasDefinition[MT <: MetaTypes](name: Option[ResourceName], label: AutoTableLabel) extends Annotation[MT]
  case class ColumnAliasDefinition[MT <: MetaTypes](name: ColumnName, label: types.ColumnLabel[MT]) extends Annotation[MT]
  case class TableDefinition[MT <: MetaTypes](label: types.AutoTableLabel[MT]) extends Annotation[MT]
  case class ColumnRef[MT <: MetaTypes](table: types.AutoTableLabel[MT], label: ColumnLabel[MT#DatabaseColumnNameImpl]) extends Annotation[MT]
  case class SelectListDefinition[MT <: MetaTypes](idx: Int) extends Annotation[MT]
  case class SelectListReference[MT <: MetaTypes](idx: Int) extends Annotation[MT]
  case class Typed[MT <: MetaTypes](typ: types.ColumnType[MT]) extends Annotation[MT]
}
