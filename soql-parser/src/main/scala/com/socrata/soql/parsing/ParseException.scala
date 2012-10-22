package com.socrata.soql.parsing

import scala.util.parsing.input.Position

import com.socrata.soql.SoQLException

class ParseException(message: String, position: Position) extends SoQLException(message, position)


