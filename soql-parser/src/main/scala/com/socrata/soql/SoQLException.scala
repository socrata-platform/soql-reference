package com.socrata.soql

import scala.util.parsing.input.Position

class SoQLException(val message: String, val position: Position) extends RuntimeException(message + ":\n" + position.longString)
