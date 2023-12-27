package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast
import com.socrata.soql.environment.{ScopedResourceName, Source}
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
              ParserError.UnexpectedEscape(Source.nonSynthetic(source, pos), char)
            case BadUnicodeEscapeCharacter(char, pos) =>
              ParserError.BadUnicodeEscapeCharacter(Source.nonSynthetic(source, pos), char)
            case UnicodeCharacterOutOfRange(value, pos) =>
              ParserError.UnicodeCharacterOutOfRange(Source.nonSynthetic(source, pos), value)
            case UnexpectedCharacter(char, pos) =>
              ParserError.UnexpectedCharacter(Source.nonSynthetic(source, pos), char)
            case UnexpectedEOF(pos) =>
              ParserError.UnexpectedEOF(Source.nonSynthetic(source, pos))
            case UnterminatedString(pos) =>
              ParserError.UnterminatedString(Source.nonSynthetic(source, pos))
          }
        case bp: BadParse =>
          bp match {
            case expectedToken: BadParse.ExpectedToken =>
              ParserError.ExpectedToken(
                Source.nonSynthetic(source, expectedToken.reader.first.position),
                expectedToken.reader.alternates.to(LazyList).map(_.printable),
                expectedToken.reader.first.quotedPrintable
              )
            case expectedLeafQuery: BadParse.ExpectedLeafQuery =>
              ParserError.ExpectedLeafQuery(Source.nonSynthetic(source, expectedLeafQuery.reader.first.position))
            case unexpectedStarSelect: BadParse.UnexpectedStarSelect =>
              ParserError.UnexpectedStarSelect(Source.nonSynthetic(source, unexpectedStarSelect.reader.first.position))
            case unexpectedSystemStarSelect: BadParse.UnexpectedSystemStarSelect =>
              ParserError.UnexpectedSystemStarSelect(Source.nonSynthetic(source, unexpectedSystemStarSelect.reader.first.position))
          }
      }
    }
  }
}
