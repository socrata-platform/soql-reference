package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

trait SqlNamespaces[MT <: MetaTypes] extends LabelUniverse[MT] {
  // Turns an AutoTableLabel into a table name that is guaranteed to
  // not conflict with any real table.
  def tableLabel(table: AutoTableLabel): Doc[Nothing] =
    d"$autoTablePrefix${table.name}"

  // Ditto, for CTE labels
  def cteLabel(cte: AutoCTELabel): Doc[Nothing] =
    d"$autoCTEPrefix${cte.name}"

  // If the label is an AutoColumnLabel, turns it into a column name
  // that is guaranteed to not conflict with any real column.
  // Otherwise it returns the (base of) the physical column(s) which
  // make up that logical column.
  def columnName(label: ColumnLabel): Doc[Nothing] =
    Doc(rawColumn(label))

  def columnName(label: ColumnLabel, suffix: String): Doc[Nothing] =
    Doc(rawColumn(label, suffix))

  def rawColumn(label: ColumnLabel): String =
    label match {
      case dcn: DatabaseColumnName => rawDatabaseColumnName(dcn)
      case acl: AutoColumnLabel => rawAutoColumnName(acl)
    }

  def rawColumn(label: ColumnLabel, suffix: String): String =
    label match {
      case dcn: DatabaseColumnName => rawDatabaseColumnName(dcn, suffix)
      case acl: AutoColumnLabel => rawAutoColumnName(acl, suffix)
    }

  def databaseTableName(dtn: DatabaseTableName): Doc[Nothing] = Doc(rawDatabaseTableName(dtn))
  def databaseColumn(dcn: DatabaseColumnName): Doc[Nothing] = Doc(rawDatabaseColumnName(dcn))
  def databaseColumn(dcn: DatabaseColumnName, suffix: String): Doc[Nothing] = Doc(rawDatabaseColumnName(dcn, suffix))
  def autoColumnName(acl: AutoColumnLabel): Doc[Nothing] = Doc(rawAutoColumnName(acl))
  def autoColumnName(acl: AutoColumnLabel, suffix: String): Doc[Nothing] = Doc(rawAutoColumnName(acl, suffix))

  def rawAutoColumnName(acl: AutoColumnLabel): String = s"$autoColumnPrefix${acl.name}"
  def rawAutoColumnName(acl: AutoColumnLabel, suffix: String): String = s"$autoColumnPrefix${acl.name}_$suffix"

  def indexName(dtn: DatabaseTableName, col: ColumnLabel): Doc[Nothing] =
    Doc(rawIndexName(dtn, col))

  def rawIndexName(dtn: DatabaseTableName, col: ColumnLabel): String =
    idxPrefix + "_" + rawDatabaseTableName(dtn) + "_" + rawColumn(col, "")

  def indexName(dtn: DatabaseTableName, col: ColumnLabel, subcol: String): Doc[Nothing] =
    Doc(rawIndexName(dtn, col, subcol))

  def rawIndexName(dtn: DatabaseTableName, col: ColumnLabel, subcol: String): String =
    rawIndexName(dtn, col) + "_" + subcol

  protected def idxPrefix: String

  protected def autoTablePrefix: String

  protected def autoCTEPrefix: String

  protected def autoColumnPrefix: String

  def rawDatabaseTableName(dtn: DatabaseTableName): String
  def rawDatabaseColumnName(dcn: DatabaseColumnName): String
  // This should create a name which is the moral equivalent of
  //   rawDatabaseColumnName(dcn) + "_" + suffix
  def rawDatabaseColumnName(dcn: DatabaseColumnName, suffix: String): String
  def gensymPrefix: String
}
