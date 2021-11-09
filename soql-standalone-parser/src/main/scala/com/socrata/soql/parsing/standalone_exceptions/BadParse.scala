package com.socrata.soql.parsing.standalone_exceptions

import scala.collection.compat.immutable.LazyList
import scala.util.parsing.input.Position

import com.socrata.soql.parsing.RecursiveDescentParser
import com.socrata.soql.parsing.RecursiveDescentParser.{Reader, ParseException}

case class BadParse(message: String, position: Position) extends Exception(message)

object BadParse {
  class ExpectedToken(val reader: Reader)
      extends BadParse(ExpectedToken.msg(reader), reader.first.position)
      with ParseException
  {
    override val position = super.position
  }

  object ExpectedToken {
    private def msg(reader: Reader) = {
      RecursiveDescentParser.expectationsToEnglish(reader.alternates, reader.first)
    }
  }

  class ExpectedLeafQuery(val reader: Reader)
      extends BadParse(ExpectedLeafQuery.msg(reader), reader.first.position)
      with ParseException
  {
    override val position = super.position
  }

  object ExpectedLeafQuery {
    private def msg(reader: Reader) = {
      "Expected a non-compound query on the right side of a pipe operator"
    }
  }

  class UnexpectedStarSelect(val reader: Reader)
      extends BadParse(UnexpectedStarSelect.msg(reader), reader.first.position)
      with ParseException
  {
    override val position = super.position
  }

  object UnexpectedStarSelect {
    private def msg(reader: Reader) = {
      "Star selections must come at the start of the select-list"
    }
  }

  class UnexpectedSystemStarSelect(val reader: Reader)
      extends BadParse(UnexpectedSystemStarSelect.msg(reader), reader.first.position)
      with ParseException
  {
    override val position = super.position
  }

  object UnexpectedSystemStarSelect {
    private def msg(reader: Reader) = {
      "System column star selections must come before user column star selections"
    }
  }
}
