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

case class NoRollup(pos: Position) extends Hint {
  def doc = Doc(toString)

  override def toString(): String = "no_rollup"
}

case class CompoundRollup(pos: Position) extends Hint {
  def doc = Doc(toString)

  override def toString(): String = "compound_rollup"
}

case class RollupAtJoin(pos: Position) extends Hint {
  def doc = Doc(toString)

  override def toString(): String = "rollup_at_join"
}

case class NoChainMerge(pos: Position) extends Hint {
  def doc = Doc(toString)

  override def toString(): String = "no_chain_merge"
}