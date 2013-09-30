package com.socrata.soql.parsing.standalone_exceptions

import scala.util.parsing.input.Position

sealed abstract class StandaloneLexerException(m: String, p: Position) extends RuntimeException(m + ":\n" + p.longString) {
  def position: Position
}

case class UnexpectedEscape(char: Char, position: Position) extends StandaloneLexerException("Unexpected escape character", position)
case class BadUnicodeEscapeCharacter(char: Char, position: Position) extends StandaloneLexerException("Bad character in unicode escape", position)
case class UnicodeCharacterOutOfRange(value: Int, position:Position) extends StandaloneLexerException("Unicode character out of range", position)
case class UnexpectedCharacter(char: Char, position: Position) extends StandaloneLexerException("Unexpected character", position)
case class UnexpectedEOF(position: Position) extends StandaloneLexerException("Unexpected end of input", position)
case class UnterminatedString(position: Position) extends StandaloneLexerException("Unterminated string", position)
