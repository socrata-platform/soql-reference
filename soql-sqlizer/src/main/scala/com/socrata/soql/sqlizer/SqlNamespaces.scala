package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

trait SqlNamespaces[MT <: MetaTypes] extends LabelUniverse[MT] {
  // Turns an AutoTableLabel into a table name that is guaranteed to
  // not conflict with any real table.
  def tableLabel(table: AutoTableLabel): Doc[Nothing] =
    d"$autoTablePrefix${table.name}"

  // If the label is an AutoColumnLabel, turns it into a column name
  // that is guaranteed to not conflict with any real column.
  // Otherwise it returns the (base of) the physical column(s) which
  // make up that logical column.
  def columnBase(label: ColumnLabel): Doc[Nothing] =
    Doc(rawColumnBase(label))

  def rawColumnBase(label: ColumnLabel): String =
    label match {
      case dcn: DatabaseColumnName => rawDatabaseColumnBase(dcn)
      case acl: AutoColumnLabel => rawAutoColumnBase(acl)
    }

  def databaseTableName(dtn: DatabaseTableName): Doc[Nothing] = Doc(rawDatabaseTableName(dtn))
  def databaseColumnBase(dcn: DatabaseColumnName): Doc[Nothing] = Doc(rawDatabaseColumnBase(dcn))
  def autoColumnBase(acl: AutoColumnLabel): Doc[Nothing] = Doc(rawAutoColumnBase(acl))

  def rawAutoColumnBase(acl: AutoColumnLabel): String = s"$autoColumnPrefix${acl.name}"

  def indexName(dtn: DatabaseTableName, col: ColumnLabel): Doc[Nothing] =
    Doc(rawIndexName(dtn, col))

  def rawIndexName(dtn: DatabaseTableName, col: ColumnLabel): String =
    idxPrefix + "_" + rawDatabaseTableName(dtn) + "_" + rawColumnBase(col)

  def indexName(dtn: DatabaseTableName, col: ColumnLabel, subcol: String): Doc[Nothing] =
    Doc(rawIndexName(dtn, col, subcol))

  def rawIndexName(dtn: DatabaseTableName, col: ColumnLabel, subcol: String): String =
    rawIndexName(dtn, col) + "_" + subcol

  protected def idxPrefix: String

  protected def autoTablePrefix: String

  protected def autoColumnPrefix: String

  def rawDatabaseTableName(dtn: DatabaseTableName): String
  def rawDatabaseColumnBase(dcn: DatabaseColumnName): String
  def gensymPrefix: String
}
