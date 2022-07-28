package com.socrata.soql.analyzer2

class LabelProvider {
  private var tables = 0
  private var columns = 0

  def tableLabel(): TableLabel = {
    tables += 1
    new TableLabel(tables)
  }
  def columnLabel(): ColumnLabel = {
    columns += 1
    new ColumnLabel(columns)
  }
}

final class TableLabel private[analyzer2] (private val n: Int) extends AnyVal {
  override def toString = s"t$n"
}
final class ColumnLabel private[analyzer2] (private val n: Int) extends AnyVal {
  override def toString = s"c$n"
}
