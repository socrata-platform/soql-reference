package com.socrata.soql.analyzer2

class LabelProvider {
  private var tables = 0
  private var columns = 0

  def tableLabel(): TableLabel = {
    tables += 1
    new AutoTableLabel(tables)
  }
  def columnLabel(): ColumnLabel = {
    columns += 1
    new AutoColumnLabel(columns)
  }
}

sealed abstract class TableLabel
final class AutoTableLabel private[analyzer2] (private val n: Int) extends TableLabel {
  override def toString = s"t$n"

  override def hashCode = n
  override def equals(that: Any) =
    that match {
      case atl: AutoTableLabel => this.n == atl.n
      case _ => false
    }
}
final case class DatabaseTableName(name: String) extends TableLabel {
  override def toString = name
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
