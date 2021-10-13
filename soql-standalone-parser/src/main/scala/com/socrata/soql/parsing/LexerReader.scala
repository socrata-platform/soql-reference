package com.socrata.soql.parsing

import scala.util.parsing.input.{Reader, NoPosition}

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.util.ULocale

import com.socrata.soql.tokens._

trait StreamableReader[T] extends Reader[T] {
  def toStream: StreamShim.Stream[T]
  override def rest: StreamableReader[T]
}

trait ExtendedReader[T] extends StreamableReader[T] {
  private var alts: List[Set[HandRolledParser.Tokenlike]] = Nil

  def alternates = alts.foldLeft(Set.empty[HandRolledParser.Tokenlike])(_ union _)
  def alternates_=(a: Set[HandRolledParser.Tokenlike]): Unit = alts ::= a
  def resetAlternates(): Unit = alts = Nil

  override def rest: ExtendedReader[T]
}

class LexerReader(lexer: AbstractLexer) extends ExtendedReader[Token] { self =>
  import LexerReader._

  val first = lexer.yylex() match {
    case i@Identifier(_, false) => keywordize(i)
    case other => other
  }

  def atEnd = false
  def pos = first.position

  def toStream: StreamShim.Stream[Token] = first #:: rest.toStream

  lazy val rest: ExtendedReader[Token] = {
    if(atEnd) this
    else if(first.isInstanceOf[EOF]) {
      new ExtendedReader[Token] {
        def first = self.first
        def rest = this
        def atEnd = true
        def pos = first.position
        def toStream = StreamShim.Stream.empty
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
    DISTINCT,
    EXCEPT,
    AS,
    WHERE,
    GROUP,
    HAVING,
    ORDER,
    LIMIT,
    OFFSET,
    SEARCH,
    ASC,
    DESC,

    // Presently unused keywords
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
    LATERAL,

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

