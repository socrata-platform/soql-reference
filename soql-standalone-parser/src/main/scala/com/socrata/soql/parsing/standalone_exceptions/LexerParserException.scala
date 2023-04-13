package com.socrata.soql.parsing.standalone_exceptions

import scala.collection.compat.immutable.LazyList
import scala.util.parsing.input.Position

import com.socrata.soql.parsing.RecursiveDescentParser
import com.socrata.soql.parsing.RecursiveDescentParser.{Reader, ParseException}

sealed abstract class LexerParserException(msg: String) extends RuntimeException(msg)

sealed abstract class StandaloneLexerException(m: String, p: Position) extends LexerParserException(m + ":\n" + p.longString) {
  def position: Position
}

case class UnexpectedEscape(char: Char, position: Position) extends StandaloneLexerException("Unexpected escape character", position)
case class BadUnicodeEscapeCharacter(char: Char, position: Position) extends StandaloneLexerException("Bad character in unicode escape", position)
case class UnicodeCharacterOutOfRange(value: Int, position:Position) extends StandaloneLexerException("Unicode character out of range", position)
case class UnexpectedCharacter(char: Char, position: Position) extends StandaloneLexerException("Unexpected character", position)
case class UnexpectedEOF(position: Position) extends StandaloneLexerException("Unexpected end of input", position)
case class UnterminatedString(position: Position) extends StandaloneLexerException("Unterminated string", position)

sealed abstract class BadParse(val message: String) extends LexerParserException(message) {
  val position: Position
}

object BadParse {
  class ExpectedToken(val reader: Reader)
      extends BadParse(ExpectedToken.msg(reader))
      with ParseException
  {
    override val position = reader.first.position
  }

  object ExpectedToken {
    private def msg(reader: Reader) = {
      RecursiveDescentParser.expectationsToEnglish(reader.alternates, reader.first)
    }
  }

  class ExpectedLeafQuery(val reader: Reader)
      extends BadParse(ExpectedLeafQuery.msg(reader))
      with ParseException
  {
    override val position = reader.first.position
  }

  object ExpectedLeafQuery {
    private def msg(reader: Reader) = {
      "Expected a non-compound query on the right side of a pipe operator"
    }
  }

  class UnexpectedStarSelect(val reader: Reader)
      extends BadParse(UnexpectedStarSelect.msg(reader))
      with ParseException
  {
    override val position = reader.first.position
  }

  object UnexpectedStarSelect {
    private def msg(reader: Reader) = {
      "Star selections must come at the start of the select-list"
    }
  }

  class UnexpectedSystemStarSelect(val reader: Reader)
      extends BadParse(UnexpectedSystemStarSelect.msg(reader))
      with ParseException
  {
    override val position = reader.first.position
  }

  object UnexpectedSystemStarSelect {
    private def msg(reader: Reader) = {
      "System column star selections must come before user column star selections"
    }
  }
}
