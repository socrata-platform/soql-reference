package com.socrata.soql.parsing

import com.socrata.soql.Position

sealed abstract class LexerError(val message: String, val position: Position) extends Exception(message + "\n" + position.longString)

class UnexpectedEscape(val char: Char, position: Position) extends LexerError("Unexpected escape character", position)

class BadUnicodeEscape(msg: String, position: Position) extends LexerError(msg, position) {
  def this(position: Position) = this("Bad unicode escape", position)
}

class UnicodeCharacterOutOfRange(val value: Int, position:Position) extends BadUnicodeEscape("Unicode character out of range", position)

class UnexpectedCharacter(val char: Char, position: Position) extends LexerError("Unexpected character", position)

class UnexpectedEOF(position: Position) extends LexerError("Unexpected end of input", position)

class UnterminatedString(position: Position) extends LexerError("Unterminated string", position)
