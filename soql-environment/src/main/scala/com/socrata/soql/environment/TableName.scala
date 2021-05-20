package com.socrata.soql.environment

case class TableName(name: String, alias: Option[String] = None) {
  override def toString(): String = {
    aliasWithoutPrefix.foldLeft(TableName.withSoqlPrefix(name))((n, a) => s"$n AS `$a`")
  }

  def qualifier: String = alias.getOrElse(name)

  /** removes any leading Soql or SF prefix ("@" or "_") from `name` */
  def nameWithoutPrefix: String = TableName.removeValidPrefix(name)

  /** removes any leading "_" (SF prefix); adds leading "@" (Soql prefix) if that isn't already the first character */
  def nameWithSoqlPrefix: String = TableName.withSoqlPrefix(name)

  /** removes any leading "@" (Soql prefix); adds leading "_" (SF prefix) if that isn't already the first character */
  def nameWithSodaFountainPrefix: String = TableName.withSodaFountainPrefix(name)

  /** removes any leading Soql or SF prefix ("@" or "_") from `alias` */
  def aliasWithoutPrefix: Option[String] = alias.map(TableName.removeValidPrefix)
}

object TableName {
  // All resource name in soda fountain have an underscore prefix.
  // that is not exposed to end users making SoQLs.
  // To handle this mis-match, we automatically prepend the prefix when the parse tree is built.
  // To remove this "feature", re-define this with an empty string "" or completely remove this variable.
  val SodaFountainPrefix = "_"
  val SoqlPrefix = "@"
  val PrimaryTable = TableName(SodaFountainPrefix)
  val This = SodaFountainPrefix + "this"
  val SingleRow = SodaFountainPrefix + "single_row"

  val reservedNames = Set(This, SingleRow)

  val Field = "."
  val PrefixIndex = 1

  val prefixRegex = s"^[$SodaFountainPrefix$SoqlPrefix]".r

  /** removes any single leading Soql or SF prefix ("@" or "_") from `s` */
  def removeValidPrefix(s: String) = prefixRegex.replaceFirstIn(s, "")

  /** removes any leading "_" (SF prefix); adds leading "@" (Soql prefix) if that isn't already the first character */
  def withSoqlPrefix(s: String): String = s"$SoqlPrefix${removeValidPrefix(s)}"

  /** removes any leading "@" (Soql prefix); adds leading "_" (SF prefix) if that isn't already the first character */
  def withSodaFountainPrefix(s: String): String = s"$SodaFountainPrefix${removeValidPrefix(s)}"
}
