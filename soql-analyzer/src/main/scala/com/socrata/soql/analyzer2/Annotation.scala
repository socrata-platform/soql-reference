package com.socrata.soql.analyzer2

import com.socrata.prettyprint.{tree, SimpleDocTree}
import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil

import com.socrata.soql.environment.{ResourceName, ColumnName}

sealed abstract class Annotation[MT <: MetaTypes]
object Annotation {
  case class TableAliasDefinition[MT <: MetaTypes](name: Option[ResourceName], label: TableLabel[MT]) extends Annotation[MT]
  case class ColumnAliasDefinition[MT <: MetaTypes](name: ColumnName, label: ColumnLabel[MT#DatabaseColumnNameImpl]) extends Annotation[MT]
  case class TableDefinition[MT <: MetaTypes](label: TableLabel[MT#DatabaseTableNameImpl]) extends Annotation[MT]
  case class ColumnRef[MT <: MetaTypes](table: TableLabel[MT#DatabaseTableNameImpl], label: ColumnLabel[MT#DatabaseColumnNameImpl]) extends Annotation[MT]
  case class SelectListDefinition[MT <: MetaTypes](idx: Int) extends Annotation[MT]
  case class SelectListReference[MT <: MetaTypes](idx: Int) extends Annotation[MT]
  case class Typed[MT <: MetaTypes](typ: MT#ColumnType) extends Annotation[MT]
}
