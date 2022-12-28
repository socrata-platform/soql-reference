package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast
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

  def parseInContext[RNS](scope: RNS, canonicalName: Option[CanonicalName], soql: String, params: AbstractParser.Parameters): Either[SoQLAnalyzerError.TextualError[RNS, SoQLAnalyzerError.ParserError], BinaryTree[ast.Select]] = {
    parseWithoutContext(soql, params).left.map { err =>
      def p(e: SoQLAnalyzerError.ParserError, p: Position) =
        SoQLAnalyzerError.TextualError(scope, canonicalName, p, e)

      err match {
        case sle: StandaloneLexerException =>
          sle match {
            case UnexpectedEscape(char, pos) =>
              p(SoQLAnalyzerError.ParserError.UnexpectedEscape(char), pos)
            case BadUnicodeEscapeCharacter(char, pos) =>
              p(SoQLAnalyzerError.ParserError.BadUnicodeEscapeCharacter(char), pos)
            case UnicodeCharacterOutOfRange(value, pos) =>
              p(SoQLAnalyzerError.ParserError.UnicodeCharacterOutOfRange(value), pos)
            case UnexpectedCharacter(char, pos) =>
              p(SoQLAnalyzerError.ParserError.UnexpectedCharacter(char), pos)
            case UnexpectedEOF(pos) =>
              p(SoQLAnalyzerError.ParserError.UnexpectedEOF, pos)
            case UnterminatedString(pos) =>
              p(SoQLAnalyzerError.ParserError.UnterminatedString, pos)
          }
        case bp: BadParse =>
          bp match {
            case expectedToken: BadParse.ExpectedToken =>
              p(
                SoQLAnalyzerError.ParserError.ExpectedToken(
                  expectedToken.reader.alternates.to(LazyList).map(_.printable),
                  expectedToken.reader.first.quotedPrintable
                ),
                expectedToken.reader.first.position
              )
            case expectedLeafQuery: BadParse.ExpectedLeafQuery =>
              p(SoQLAnalyzerError.ParserError.ExpectedLeafQuery, expectedLeafQuery.reader.first.position)
            case unexpectedStarSelect: BadParse.UnexpectedStarSelect =>
              p(SoQLAnalyzerError.ParserError.UnexpectedStarSelect, unexpectedStarSelect.reader.first.position)
            case unexpectedSystemStarSelect: BadParse.UnexpectedSystemStarSelect =>
              p(SoQLAnalyzerError.ParserError.UnexpectedSystemStarSelect, unexpectedSystemStarSelect.reader.first.position)
          }
      }
    }
  }
}
