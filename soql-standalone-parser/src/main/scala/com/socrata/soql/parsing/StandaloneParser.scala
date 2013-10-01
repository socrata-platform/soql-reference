package com.socrata.soql.parsing

import com.socrata.soql.parsing.standalone_exceptions.BadParse
import scala.util.parsing.input.Position
import java.io.StringReader

class StandaloneParser extends AbstractParser {
  protected def badParse(msg: String, nextPos: Position): Nothing =
    throw BadParse(msg, nextPos)

  protected def lexer(s: String): AbstractLexer = new StandaloneLexer(s)
}
