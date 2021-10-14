package com.socrata.soql.parsing

import scala.util.parsing.input.Position
import com.socrata.soql.exceptions.RecursiveDescentBadParse

class Parser(parameters: AbstractParser.Parameters = AbstractParser.defaultParameters) extends RecursiveDescentParser(parameters) {
  protected def expected(reader: RecursiveDescentParser.Reader) =
    new RecursiveDescentBadParse(reader)

  override protected def lexer(s: String): AbstractLexer = new Lexer(s)
}
