package com.socrata.soql.parsing

import scala.util.parsing.input.Position
import com.socrata.soql.exceptions.BadParse

class Parser(parameters: AbstractParser.Parameters = AbstractParser.defaultParameters) extends RecursiveDescentParser(parameters) {
  protected def expected(reader: RecursiveDescentParser.Reader) =
    new BadParse.ExpectedToken(reader)

  protected def expectedLeafQuery(reader: RecursiveDescentParser.Reader) =
    new BadParse.ExpectedLeafQuery(reader)

  protected def unexpectedStarSelect(reader: RecursiveDescentParser.Reader) =
    new BadParse.UnexpectedStarSelect(reader)

  protected def unexpectedSystemStarSelect(reader: RecursiveDescentParser.Reader) =
    new BadParse.UnexpectedSystemStarSelect(reader)

  override protected def lexer(s: String): AbstractLexer = new Lexer(s)
}
