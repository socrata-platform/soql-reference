package com.socrata.soql.parsing

import scala.util.parsing.input.{Reader, NoPosition}

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.util.ULocale

import com.socrata.soql.tokens._

class LexerReader(lexer: Lexer) extends Reader[Token] { self =>
  import LexerReader._

  def this(soql: String) = this(new Lexer(soql))

  val first = lexer.yylex() match {
    case i@Identifier(_, false) => keywordize(i)
    case other => other
  }

  def atEnd = first.isInstanceOf[EOF]
  def pos = first.position

  def toStream: Stream[Token] = if(atEnd) Stream.empty else first #:: rest.toStream

  lazy val rest = {
    if(atEnd) this
    else new LexerReader(lexer)
  }
}

object LexerReader {
  private def keywordize(identifier: Identifier) =
    keywords.get(UCharacter.toLowerCase(keywordLocale, identifier.value)) match {
      case Some(keywordFactory) =>
        val token = keywordFactory()
        token.position = identifier.position
        token
      case None =>
        identifier
    }

  private val keywordLocale = ULocale.ENGLISH

  private val keywords = List(
    SELECT,
    EXCEPT,
    AS,
    WHERE,
    GROUP,
    BY,
    HAVING,
    ORDER,
    LIMIT,
    OFFSET,
    ASC,
    DESC,
    FIRST,
    LAST,

    // Presently unused keywords
    DISTINCT,
    FROM,
    FULL,
    IN,
    INNER,
    JOIN,
    LEFT,
    RIGHT,
    ON,
    OUTER,
    INNER,

    // Boolean operators
    AND,
    OR,
    NOT,

    // Weird boolean-valued functions with special syntax
    IS,
    BETWEEN,
    LIKE,

    // Literals
    () => BooleanLiteral(true),
    () => BooleanLiteral(false),
    NULL
  ).foldLeft(Map.empty[String, () => Token]) { (acc, tokenFactory) =>
    acc + (tokenFactory().printable.toLowerCase -> tokenFactory)
  }
}

