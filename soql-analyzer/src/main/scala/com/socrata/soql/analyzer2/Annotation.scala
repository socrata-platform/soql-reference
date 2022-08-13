package com.socrata.soql.analyzer2

import com.socrata.prettyprint.{tree, SimpleDocTree}
import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil

import com.socrata.soql.environment.{ResourceName, ColumnName}

sealed abstract class Annotation[+RNS, +CT]
object Annotation {
  case class TableAliasDefinition[+RNS](name: Option[(RNS, ResourceName)], label: TableLabel) extends Annotation[RNS, Nothing]
  case class ColumnAliasDefinition(name: ColumnName, label: ColumnLabel) extends Annotation[Nothing, Nothing]
  case class TableDefinition(label: TableLabel) extends Annotation[Nothing, Nothing]
  case class ColumnRef(table: TableLabel, label: ColumnLabel) extends Annotation[Nothing, Nothing]
  case class SelectListDefinition(idx: Int) extends Annotation[Nothing, Nothing]
  case class SelectListReference(idx: Int) extends Annotation[Nothing, Nothing]
  case class Typed[+CT](typ: CT) extends Annotation[Nothing, CT]
}
