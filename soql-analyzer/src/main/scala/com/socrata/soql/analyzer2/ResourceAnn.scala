package com.socrata.soql.analyzer2

import com.socrata.prettyprint.{tree, SimpleDocTree}
import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil

import com.socrata.soql.environment.{ResourceName, ColumnName}

sealed abstract class ResourceAnn[+RNS, +CT]
object ResourceAnn {
  def from[RNS](alias: Option[(RNS, ResourceName)], label: TableLabel) =
    UserTableAlias(alias, label)

  def from[RNS, CT](alias: ColumnName, label: ColumnLabel) =
    UserColumnAlias(alias, label)

  def from[RNS, CT](table: TableLabel, label: ColumnLabel) =
    ColumnRef(table, label)

  def from[CT](typ: CT) = Typed(typ)

  case class UserTableAlias[+RNS](name: Option[(RNS, ResourceName)], label: TableLabel) extends ResourceAnn[RNS, Nothing]
  case class UserColumnAlias(name: ColumnName, label: ColumnLabel) extends ResourceAnn[Nothing, Nothing]
  case class TableDef(label: TableLabel) extends ResourceAnn[Nothing, Nothing]
  case class ColumnDef(label: ColumnLabel) extends ResourceAnn[Nothing, Nothing]
  case class ColumnRef(table: TableLabel, label: ColumnLabel) extends ResourceAnn[Nothing, Nothing]
  case class Typed[+CT](typ: CT) extends ResourceAnn[Nothing, CT]
}
