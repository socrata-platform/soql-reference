package com.socrata.soql.parsing.standalone_exceptions

import scala.util.parsing.input.Position

case class BadParse(message: String, position: Position) extends Exception(message)
