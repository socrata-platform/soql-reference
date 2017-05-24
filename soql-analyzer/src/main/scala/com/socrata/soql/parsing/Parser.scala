package com.socrata.soql.parsing

import scala.util.parsing.input.Position
import com.socrata.soql.exceptions.BadParse

class Parser(parameters: AbstractParser.Parameters = AbstractParser.defaultParameters) extends StandaloneParser(parameters) {
  override protected def badParse(msg: String, position: Position): Nothing =
    throw BadParse(msg, position)
}
