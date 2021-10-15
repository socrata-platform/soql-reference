package com.socrata.soql.parsing.standalone_exceptions

import scala.collection.compat.immutable.LazyList
import scala.util.parsing.input.Position

import com.socrata.soql.parsing.RecursiveDescentParser
import com.socrata.soql.parsing.RecursiveDescentParser.{Reader, ParseException}

case class BadParse(message: String, position: Position) extends Exception(message)

class RecursiveDescentBadParse(val reader: Reader)
    extends BadParse(RecursiveDescentBadParse.msg(reader), reader.first.position)
    with ParseException
{
  override val position = super.position
}

object RecursiveDescentBadParse {
  private def msg(reader: Reader) = {
    RecursiveDescentParser.expectationsToEnglish(reader.alternates, reader.first)
  }
}
