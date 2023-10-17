package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast
import com.socrata.soql.environment.ScopedResourceName
import com.socrata.soql.parsing.standalone_exceptions._
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser, RecursiveDescentParser}

private[analyzer2] object ParserUtil {
  def parseWithoutContext(soql: String, params: AbstractParser.Parameters): Either[LexerParserException, BinaryTree[ast.Select]] =
    try {
      Right(new StandaloneParser(params).binaryTreeSelect(soql))
    } catch {
      case e: LexerParserException =>
        Left(e)
    }

  def parseInContext[RNS](source: Option[ScopedResourceName[RNS]], soql: String, params: AbstractParser.Parameters): Either[ParserError[RNS], BinaryTree[ast.Select]] = {
    parseWithoutContext(soql, params).left.map { err =>
      err match {
        case sle: StandaloneLexerException =>
          sle match {
            case UnexpectedEscape(char, pos) =>
              ParserError.UnexpectedEscape(source, pos, char)
            case BadUnicodeEscapeCharacter(char, pos) =>
              ParserError.BadUnicodeEscapeCharacter(source, pos, char)
            case UnicodeCharacterOutOfRange(value, pos) =>
              ParserError.UnicodeCharacterOutOfRange(source, pos, value)
            case UnexpectedCharacter(char, pos) =>
              ParserError.UnexpectedCharacter(source, pos, char)
            case UnexpectedEOF(pos) =>
              ParserError.UnexpectedEOF(source, pos)
            case UnterminatedString(pos) =>
              ParserError.UnterminatedString(source, pos)
          }
        case bp: BadParse =>
          bp match {
            case expectedToken: BadParse.ExpectedToken =>
              ParserError.ExpectedToken(
                source,
                expectedToken.reader.first.position,
                expectedToken.reader.alternates.to(LazyList).map(_.printable),
                expectedToken.reader.first.quotedPrintable
              )
            case expectedLeafQuery: BadParse.ExpectedLeafQuery =>
              ParserError.ExpectedLeafQuery(source, expectedLeafQuery.reader.first.position)
            case unexpectedStarSelect: BadParse.UnexpectedStarSelect =>
              ParserError.UnexpectedStarSelect(source, unexpectedStarSelect.reader.first.position)
            case unexpectedSystemStarSelect: BadParse.UnexpectedSystemStarSelect =>
              ParserError.UnexpectedSystemStarSelect(source, unexpectedSystemStarSelect.reader.first.position)
          }
      }
    }
  }
}
