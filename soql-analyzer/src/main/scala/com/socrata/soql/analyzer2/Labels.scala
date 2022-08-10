package com.socrata.soql.analyzer2

class LabelProvider(tablePattern: Int => String, columnPattern: Int => String) {
  private var tables = 0
  private var columns = 0

  def tableLabel(): AutoTableLabel = {
    tables += 1
    new AutoTableLabel(tablePattern(tables))
  }
  def columnLabel(): AutoColumnLabel = {
    columns += 1
    new AutoColumnLabel(columnPattern(columns))
  }
}

sealed abstract class TableLabel
final class AutoTableLabel private[analyzer2] (private val name: String) extends TableLabel {
  override def toString = name

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case atl: AutoTableLabel => this.name == atl.name
      case _ => false
    }
}
object AutoTableLabel {
  def forTest(name: String) = new AutoTableLabel(name)
}
final case class DatabaseTableName(name: String) extends TableLabel {
  override def toString = name
}
sealed abstract class ColumnLabel
final class AutoColumnLabel private[analyzer2] (private val name: String) extends ColumnLabel {
  override def toString = name

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case acl: AutoColumnLabel => this.name == acl.name
      case _ => false
    }
}
object AutoColumnLabel {
  def forTest(name: String) = new AutoColumnLabel(name)
}
final case class DatabaseColumnName(name: String) extends ColumnLabel {
  override def toString = name
}
