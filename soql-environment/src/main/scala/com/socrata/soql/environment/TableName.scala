package com.socrata.soql.environment

case class TableName(name: String, alias: Option[String] = None) {
  import TableName._

  override def toString(): String = {
    alias.map(removePrefix).foldLeft(replacePrefixWithAt(name))((n, a) => s"$n AS $a")
  }

  def qualifier: String = alias.getOrElse(name)
}

object TableName {
  // All resource name in soda fountain have an underscore prefix.
  // that is not exposed to end users making SoQLs.
  // To handle this mis-match, we automatically prepend the prefix when the parse tree is built.
  // To remove this "feature", re-define this with an empty string "" or completely remove this variable.
  val SodaFountainPrefix = "_"
  val PrimaryTable = TableName(SodaFountainPrefix)
  val Prefix = "@"
  val Field = "."
  val PrefixIndex = 1

  def removePrefix(s: String): String = s.substring(PrefixIndex)
  def withAtPrefix(s: String): String = s"$Prefix$s"
  def withSodaFountainPrefix(s: String): String = s"$SodaFountainPrefix$s"
  def replacePrefixWithAt(s: String): String = withAtPrefix(removePrefix(s))
}
