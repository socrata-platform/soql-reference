package com.socrata.soql.environment
import TableName.Prefixers._

import scala.util.matching.Regex

case class TableName(name: String, alias: Option[String] = None) {
  override def toString(): String = {
    aliasWithoutPrefix.foldLeft(FourBy4.withSoqlPrefix(name))((n, a) => s"$n AS $a")
  }

  def qualifier: String = alias.getOrElse(name)

  /** removes any leading single character prefix from `name`, if it is an otherwise-valid 4x4. else returns `name` */
  def nameWithoutPrefix: String = FourBy4.removePrefix(name)

  /** if `name` is a (prefixed) valid 4x4, prepend (or replace existing prefix with) "@". else return `name` */
  def nameWithSoqlPrefix: String = FourBy4.withSoqlPrefix(name)

  /** if `name` is a (prefixed) valid 4x4, prepend (or replace existing prefix with) "_". else return `name` */
  def nameWithSodaFountainPrefix: String = FourBy4.withSodaFountainPrefix(name)

  /** removes prefix from `alias` if that prefix is "@" or "_" (Soql or SodaFountain prefix) */
  def aliasWithoutPrefix: Option[String] = alias.map(Alias.removePrefix)
}

object TableName {
  // All resource name in soda fountain have an underscore prefix.
  // that is not exposed to end users making SoQLs.
  // To handle this mis-match, we automatically prepend the prefix when the parse tree is built.
  // To remove this "feature", re-define this with an empty string "" or completely remove this variable.
  val SodaFountainPrefix = "_"
  val SoqlPrefix = "@"
  val PrimaryTable = TableName(SodaFountainPrefix)
  val Field = "."
  val PrefixIndex = 1

  /** Helpers to add/remove/modify prefixes to 4x4s and aliases found in TableNames */
  object Prefixers {
    private val valid4x4Chars = "[2-9a-kmnp-z]"
    private val fourBy4Regex = s"[$valid4x4Chars]{4}\\-[$valid4x4Chars]{4}".r
    private val sodaOrSoqlPrefixRegex = s"[$SodaFountainPrefix$SoqlPrefix]".r

    /**
      * Add/remove/replace any single character prefix to any valid 4x4. 4x4s have a very specific format, so we can
      * assume any single character that prefixes an otherwise-valid 4x4 is meant to be a prefix.
      */
    val FourBy4 = new PrefixReplacer(".".r, fourBy4Regex)

    /**
      * Add/remove/replace soda or soql prefix to any string at all. Aliases can be essentially any string, so we can
      * only modify known prefixes.
      */
    val Alias = new PrefixReplacer(sodaOrSoqlPrefixRegex, ".*".r)

    /**
      * Removes/adds/replaces prefixes from/to/in strings. Methods will only modify a given string `s` that matches
      * `bodyRegex` (possibly prefixed by a prefix matching `prefixRegex`). If a provided string does not match
      * `bodyRegex` (with possible prefix), then the original string will be returned unmodified by all methods.
      *
      * @param prefixRegex matches prefixes to strings given to methods. Will only remove/modify prefixes to
      *                    String function parameters that match this regex.
      * @param bodyRegex matches the body of String function parameters `s`. Only strings given that match this
      *                  regex will be modified.
      */
    protected class PrefixReplacer(prefixRegex: Regex, bodyRegex: Regex) {
      val possiblyPrefixed = s"^${prefixRegex.pattern.pattern}?(${bodyRegex.pattern.pattern})$$".r

      /** removes any matching prefixes to an `s` that matches `bodyRegex`. else returns `s` */
      def removePrefix(s: String) = possiblyPrefixed.replaceFirstIn(s, "$1")

      /** if `s` matches `bodyRegex`, prepend (or replace existing matching prefix with) "@". else returns `s` */
      def withSoqlPrefix(s: String): String = possiblyPrefixed.replaceFirstIn(s, s"$SoqlPrefix$$1")

      /** if `s` matches `bodyRegex`, prepend (or replace existing matching prefix with) "_". else returns `s` */
      def withSodaFountainPrefix(s: String): String = possiblyPrefixed.replaceFirstIn(s, s"$SodaFountainPrefix$$1")
    }
  }
}
