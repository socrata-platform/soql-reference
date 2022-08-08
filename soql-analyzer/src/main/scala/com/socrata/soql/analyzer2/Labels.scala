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
    new AutoColumnLabel(columns)
  }
}

final class TableLabel private[analyzer2] (private val n: Int) extends AnyVal {
  override def toString = s"t$n"
}
sealed abstract class ColumnLabel
final class AutoColumnLabel private[analyzer2] (private val n: Int) extends ColumnLabel {
  override def toString = s"c$n"

  override def hashCode = n
  override def equals(that: Any) =
    that match {
      case acl: AutoColumnLabel => this.n == acl.n
      case _ => false
    }
}
final case class DatabaseColumnName(name: String) extends ColumnLabel {
  override def toString = name
}
