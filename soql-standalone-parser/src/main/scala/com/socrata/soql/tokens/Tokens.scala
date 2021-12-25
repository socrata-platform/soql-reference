package com.socrata.soql.tokens

import java.math.MathContext

import scala.util.parsing.input.{Position, NoPosition, Positional}

sealed abstract class Token extends Positional {
  def position = pos
  def position_=(newPos: Position) = pos = newPos

  def setPosition(pos: Position): Unit = { position = pos }
  def printable: String = getClass.getSimpleName
  def quotedPrintable: String = "`" + printable + "'"
}

sealed abstract class FormattedToken(override val printable: String) extends Token
sealed trait LiteralToken extends Token
sealed abstract class ValueToken[T] extends Token {
  def value: T
  override def printable = value.toString
}

// Keywords
case class SELECT() extends Token
case class DISTINCT() extends Token
case class EXCEPT() extends Token
case class AS() extends Token
case class WHERE() extends Token
case class GROUP() extends Token
case class HAVING() extends Token
case class ORDER() extends Token
case class LIMIT() extends Token
case class OFFSET() extends Token
case class SEARCH() extends Token
case class FROM() extends Token
case class FILTER() extends Token

// Joins
case class JOIN() extends Token
case class ON() extends Token
case class INNER() extends Token
case class OUTER() extends Token
case class LEFT() extends Token
case class RIGHT() extends Token
case class FULL() extends Token
case class LATERAL() extends Token

sealed trait OrderDirection
case class ASC() extends Token with OrderDirection
case class DESC() extends Token with OrderDirection

// Query chaining
case class QUERYPIPE() extends FormattedToken("|>")

case class QUERYUNION() extends FormattedToken("UNION")
case class QUERYINTERSECT() extends FormattedToken("INTERSECT")
case class QUERYMINUS() extends FormattedToken("MINUS")
case class QUERYUNIONALL() extends FormattedToken("UNION ALL")
case class QUERYINTERSECTALL() extends FormattedToken("INTERSECT ALL")
case class QUERYMINUSALL() extends FormattedToken("MINUS ALL")

// Subscripting
// DOT() share with qualifying
case class LBRACKET() extends FormattedToken("[")
case class RBRACKET() extends FormattedToken("]")

// Qualifying
case class DOT() extends FormattedToken(".")

// Math
case class PLUS() extends FormattedToken("+")
case class MINUS() extends FormattedToken("-")
case class STAR() extends FormattedToken("*")
case class SLASH() extends FormattedToken("/")
case class TILDE() extends FormattedToken("~")
case class CARET() extends FormattedToken("^")
case class PERCENT() extends FormattedToken("%")

// Misc expression-y stuff
case class PIPEPIPE() extends FormattedToken("||")
case class COLONCOLON() extends FormattedToken("::")
case class LPAREN() extends FormattedToken("(")
case class RPAREN() extends FormattedToken(")")

// Comparisons
case class LESSTHAN() extends FormattedToken("<")
case class LESSTHANOREQUALS() extends FormattedToken("<=")
case class EQUALS() extends FormattedToken("=")
case class GREATERTHANOREQUALS() extends FormattedToken(">=")
case class GREATERTHAN() extends FormattedToken(">")
case class LESSGREATER() extends FormattedToken("<>")
case class EQUALSEQUALS() extends FormattedToken("==")
case class BANGEQUALS() extends FormattedToken("!=")

// Other primitive Boolean operators
case class IS() extends Token
case class BETWEEN() extends Token
case class IN() extends Token
case class LIKE() extends Token

// Boolean combinators -- except for "BANG" these are all actual words
case class AND() extends Token
case class OR() extends Token
case class NOT() extends Token
case class BANG() extends FormattedToken("!")

// Literals

case class NULL() extends Token with LiteralToken
case class NumberLiteral(value: BigDecimal) extends ValueToken[BigDecimal] with LiteralToken {
  def this(value: java.math.BigDecimal) = this(new BigDecimal(value))
}

// Scala has default math context precision of 34.  Use unlimited here.
class IntegerLiteral(val asInt: BigInt) extends NumberLiteral(BigDecimal(asInt, MathContext.UNLIMITED))
case class BooleanLiteral(value: Boolean) extends ValueToken[Boolean] with LiteralToken
case class StringLiteral(value: String) extends ValueToken[String] with LiteralToken

// Identifiers
case class Identifier(value: String, quoted: Boolean) extends ValueToken[String] {
  override def printable = if(quoted) "`" + super.printable + "`" else super.printable
}
case class SystemIdentifier(value: String, quoted: Boolean) extends ValueToken[String] {
  override def printable = if(quoted) "`" + super.printable + "`" else super.printable
}
case class TableIdentifier(value: String) extends ValueToken[String] // For #abcd-2345 syntax

case class HoleIdentifier(value: String) extends ValueToken[String] { // For ?hole syntax
  override def printable = "?" + super.printable
}

case class Hint(value: String) extends ValueToken[String] { // For block comment style hint
  override def printable = s"/*+${super.printable}*/"
}

// Punctuation
case class COMMA() extends FormattedToken(",")
case class COLONSTAR() extends FormattedToken(":*")

case class EOF() extends Token {
  override def printable = "end of input"
  override def quotedPrintable = printable
}
