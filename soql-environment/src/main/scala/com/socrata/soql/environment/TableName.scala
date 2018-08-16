package com.socrata.soql.environment

trait TableSource
case class TableName(name: String) extends TableSource {
  override def toString: String = TableName.replaceSodaPrefix(name)
}

object TableName {
  val PrimaryTable = TableName("_")

  val Prefix = "@"

  val Field = "."

  // All resource name in soda fountain have an underscore prefix.
  // that is not exposed to end users making SoQLs.
  // To handle this mis-match, we automatically prepend the prefix when the parse tree is built.
  // To remove this "feature", re-define this with an empty string "" or completely remove this variable.
  val SodaFountainTableNamePrefix = "_"
  val SodaFountainTableNamePrefixSubStringIndex = 1

  def withSodaFountainPrefix(s: String) = s"$SodaFountainTableNamePrefix$s"
  def replaceSodaPrefix(s: String) = s.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix)
  def removeSodaPrefix(s: String) = s.replaceFirst(TableName.SodaFountainTableNamePrefix, "")
}
