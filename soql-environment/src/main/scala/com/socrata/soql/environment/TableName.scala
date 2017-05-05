package com.socrata.soql.environment

case class TableName(name: String, alias: Option[String] = None) {

  import TableName._

  override def toString(): String = {
    val unPrefixedName = name.substring(SodaFountainTableNamePrefixSubStringIndex)
    "@" + unPrefixedName + alias.map(" AS " + _.substring(SodaFountainTableNamePrefixSubStringIndex)).getOrElse("")
  }

  def qualifier: String = alias.getOrElse(name)
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
}
