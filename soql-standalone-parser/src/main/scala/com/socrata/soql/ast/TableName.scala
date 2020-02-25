package com.socrata.soql.ast

import scala.util.parsing.input.Position

import com.socrata.soql.environment.ResourceName

case class TableName(resourceName: ResourceName)(val position: Position) {
  override def toString = "@" + resourceName.name
}

case class AliasedTable(tableName: TableName, alias: Option[TableName]) {
  override def toString = tableName + alias.fold("") { case TableName(rn) => " AS " + rn.name }
}
