package com.socrata.soql.parsing

import scala.collection.immutable.ListSet
import scala.util.parsing.input.{Reader, NoPosition}

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.util.ULocale

import com.socrata.soql.tokens._

trait StreamableReader[T] extends Reader[T] {
  def toStream: StreamShim.Stream[T]
  def withoutComments: StreamableReader[T]
  override def rest: StreamableReader[T]
}

trait ExtendedReader[T] extends StreamableReader[T] {
  // This is a bit of a hack used by the hand-rolled parser.  Some
  // productions choose not to consume anything.  When this happens
  // they push a set of things that they would have accepted onto this
  // list so that if an error occurs _while this token is still under
  // consideration_ the error can say "..but maybe if you did this
  // other thing it would have worked."
  //
  // Conceptually this is a single set of expectations, but these get
  // added to the list a _lot_ (e.g., every nested sub expression adds
  // to this set several times) and we only ever care about the final
  // set if an error actually occurs, so to keep overhead down we
  // reduce the cost of tracking that to a single cons-cell allocation
  // and only actually build the final set if required.
  private var alts: List[Set[RecursiveDescentParser.Expectation]] = Nil

  def alternates = alts.foldLeft(ListSet.empty[RecursiveDescentParser.Expectation])(_ union _)
  def addAlternates(a: Set[RecursiveDescentParser.Expectation]): Unit = alts ::= a
  def resetAlternates(): Unit = alts = Nil

  override def rest: ExtendedReader[T]

  override def withoutComments: ExtendedReader[T]
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
        def withoutComments = this
      }
    } else new LexerReader(lexer)
  }

  override def withoutComments: ExtendedReader[Token] = {
    var here: ExtendedReader[Token] = this
    while(here.first.isInstanceOf[Comment]) here = here.rest
    new ExtendedReader[Token] {
      def first = here.first
      lazy val rest = here.rest.withoutComments
      def atEnd = here.atEnd
      def pos = first.position
      def toStream = first #:: rest.toStream
      def withoutComments = this
    }
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
    FILTER,

    // FROM-related keywords
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

