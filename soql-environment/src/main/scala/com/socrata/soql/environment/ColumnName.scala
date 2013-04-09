package com.socrata.soql.environment

final class ColumnName(name: String) extends AbstractName[ColumnName](name) {
  protected def hashCodeSeed = 0x342a3466
}

object ColumnName extends (String => ColumnName) {
  def apply(columnName: String) = new ColumnName(columnName)
}
