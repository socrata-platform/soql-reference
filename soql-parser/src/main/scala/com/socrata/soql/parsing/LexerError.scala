package com.socrata.soql.parsing

import com.socrata.soql.Position

sealed abstract class LexerError(val position: Position) extends Exception

class UnexpectedEscape(val char: Char, position: Position) extends LexerError(position)

class BadUnicodeEscape(position: Position) extends LexerError(position)

class UnicodeCharacterOutOfRange(val value: Int, position:Position) extends BadUnicodeEscape(position)

class UnexpectedCharacter(val char: Char, position: Position) extends LexerError(position)

class UnexpectedEOF(position: Position) extends LexerError(position)

class UnterminatedString(position: Position) extends LexerError(position)
