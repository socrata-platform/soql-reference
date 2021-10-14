package com.socrata.soql.parsing.standalone_exceptions

import scala.util.parsing.input.Position

import com.socrata.soql.parsing.RecursiveDescentParser.{Reader, ParseException}

case class BadParse(message: String, position: Position) extends Exception(message)

class HandRolledBadParse(val reader: Reader)
    extends BadParse(HandRolledBadParse.msg(reader), reader.first.position)
    with ParseException
{
  override val position = super.position
}

object HandRolledBadParse {
  private def msg(reader: Reader) = ""
}
