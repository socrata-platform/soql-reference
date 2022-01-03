package com.socrata.soql.ast

import scala.util.parsing.input.Position

import com.socrata.prettyprint.prelude._

sealed abstract class Hint {
  def doc: Doc[Nothing]
}

case class Materialized(pos: Position) extends Hint {
  def doc = Doc(toString)

  override def toString(): String = "materialized"
}
