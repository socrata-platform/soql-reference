package com.socrata.soql.parsing

import com.socrata.soql.parsing.standalone_exceptions._
import scala.util.parsing.input.Position

class StandaloneLexer(soql: String) extends AbstractLexer(soql) {
  protected [parsing] def unexpectedEOF(pos: Position) = new UnexpectedEOF(pos)
  protected [parsing] def unexpectedEscape(c: Char, pos: Position) = new UnexpectedEscape(c, pos)
  protected [parsing] def unterminatedString(pos: Position) = new UnterminatedString(pos)
  protected [parsing] def badUnicodeEscapeCharacter(c: Char, pos: Position) = new BadUnicodeEscapeCharacter(c, pos)
  protected [parsing] def unicodeCharacterOutOfRange(codepoint: Int, pos: Position) = new UnicodeCharacterOutOfRange(codepoint, pos)
  protected [parsing] def unexpectedCharacter(c: Char, pos: Position) = new UnexpectedCharacter(c, pos)
}
