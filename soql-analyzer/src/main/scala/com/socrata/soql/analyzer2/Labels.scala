package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

class LabelProvider {
  private var tables = 0
  private var columns = 0

  def tableLabel(): AutoTableLabel = {
    tables += 1
    new AutoTableLabel(tables)
  }
  def columnLabel(): AutoColumnLabel = {
    columns += 1
    new AutoColumnLabel(columns)
  }
}

sealed abstract class TableLabel {
  def debugDoc: Doc[Nothing] = Doc(toString)
}
final class AutoTableLabel private[analyzer2] (private val name: Int) extends TableLabel {
  override def toString = s"t<$name>"

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case atl: AutoTableLabel => this.name == atl.name
      case _ => false
    }
}
object AutoTableLabel {
  def unapply(atl: AutoTableLabel): Some[Int] = Some(atl.name)

  def forTest(name: Int) = new AutoTableLabel(name)
}
final case class DatabaseTableName(name: String) extends TableLabel {
  override def toString = name
}
sealed abstract class ColumnLabel {
  def debugDoc: Doc[Nothing] = Doc(toString)
}
final class AutoColumnLabel private[analyzer2] (private val name: Int) extends ColumnLabel {
  override def toString = s"c<$name>"

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case acl: AutoColumnLabel => this.name == acl.name
      case _ => false
    }
}
object AutoColumnLabel {
  def unapply(atl: AutoColumnLabel): Some[Int] = Some(atl.name)

  def forTest(name: Int) = new AutoColumnLabel(name)
}
final case class DatabaseColumnName(name: String) extends ColumnLabel {
  override def toString = name
}
