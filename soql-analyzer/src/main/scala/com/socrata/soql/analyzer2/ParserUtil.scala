package com.socrata.soql.analyzer2

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.Select
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}

private[analyzer2] object ParserUtil {
  def apply(soql: String, params: AbstractParser.Parameters): Either[LexerParserException, BinaryTree[Select]] =
    try {
      Right(new StandaloneParser(params).binaryTreeSelect(soql))
    } catch {
      case e: LexerParserException =>
        Left(e)
    }
}
