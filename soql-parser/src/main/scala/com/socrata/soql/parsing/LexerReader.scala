package com.socrata.soql.parsing

import scala.util.parsing.input.{Reader, NoPosition}

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.util.ULocale

import com.socrata.soql.tokens._
import scala.Some
import scala.Some
import scala.Some

trait StreamableReader[T] extends Reader[T] {
  def toStream: Stream[T]
}

class LexerReader(lexer: AbstractLexer) extends StreamableReader[Token] { self =>
  import LexerReader._

  val first = lexer.yylex() match {
    case i@Identifier(_, false) => keywordize(i)
    case other => other
  }

  def atEnd = false
  def pos = first.position

  def toStream: Stream[Token] = first #:: rest.toStream

  lazy val rest: StreamableReader[Token] = {
    if(atEnd) this
    else if(first.isInstanceOf[EOF]) {
      new StreamableReader[Token] {
        def first = self.first
        def rest = this
        def atEnd = true
        def pos = first.position
        def toStream = Stream.empty
      }
    } else new LexerReader(lexer)
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
    SEARCH,
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

