package com.socrata.soql.environment

final class ColumnName(qualifier: Option[String], name: String) extends AbstractName[ColumnName](qualifier, name) {
  protected def hashCodeSeed = 0x342a3466
}

object ColumnName
  extends ((Option[String], String) => ColumnName)
     with (String => ColumnName) {

  def apply(qualifier: Option[String], columnName: String) = new ColumnName(qualifier, columnName)

  def apply(columnName: String) = new ColumnName(None, columnName)
}
