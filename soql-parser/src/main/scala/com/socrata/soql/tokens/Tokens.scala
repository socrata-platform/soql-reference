package com.socrata.soql.tokens

import scala.util.parsing.input.{Position, NoPosition}

sealed abstract class Token {
  var position: Position = NoPosition
  def setPosition(pos: Position) { position = pos }
  def printable: String = getClass.getSimpleName
}

sealed abstract class FormattedToken(override val printable: String) extends Token
sealed trait LiteralToken extends Token
sealed abstract class ValueToken[T] extends Token {
  def value: T
  override def printable = value.toString
}

// Keywords
case class SELECT() extends Token
case class EXCEPT() extends Token
case class AS() extends Token
case class WHERE() extends Token
case class GROUP() extends Token
case class BY() extends Token
case class HAVING() extends Token
case class ORDER() extends Token
case class LIMIT() extends Token
case class OFFSET() extends Token

sealed trait OrderDirection
case class ASC() extends Token with OrderDirection
case class DESC() extends Token with OrderDirection

sealed trait NullPlacement
case class FIRST() extends Token with NullPlacement
case class LAST() extends Token with NullPlacement

// Presently unused keywords
case class DISTINCT() extends Token
case class FROM() extends Token
case class FULL() extends Token
case class IN() extends Token
case class INNER() extends Token
case class JOIN() extends Token
case class LEFT() extends Token
case class ON() extends Token
case class OUTER() extends Token
case class RIGHT() extends Token

// Subscripting
case class DOT() extends FormattedToken(".")
case class LBRACKET() extends FormattedToken("[")
case class RBRACKET() extends FormattedToken("]")

// Math
case class PLUS() extends FormattedToken("+")
case class MINUS() extends FormattedToken("-")
case class STAR() extends FormattedToken("*")
case class SLASH() extends FormattedToken("/")
case class TILDE() extends FormattedToken("~")

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

// Other primitive Boolean operators
case class IS() extends Token
case class BETWEEN() extends Token

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
class IntegerLiteral(val asInt: BigInt) extends NumberLiteral(BigDecimal(asInt))
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

// Punctuation
case class COMMA() extends Token
case class COLONSTAR() extends FormattedToken(":*")

case class EOF() extends Token
