package com.socrata.soql.typed

import scala.util.parsing.input.Position

sealed abstract class Hint[+ColumnId, +Type] extends Product with TypableOption[Type] {
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): Hint[NewColumnId, Type]
}

case class Materialized(position: Position) extends Hint[Nothing, Nothing] {
  def typ = None

  def mapColumnIds[NewColumnId](f: (Nothing, Qualifier) => NewColumnId) = this

  override def toString: String = "materialized"
}

case class NoRollup(position: Position) extends Hint[Nothing, Nothing] {
  def typ = None

  def mapColumnIds[NewColumnId](f: (Nothing, Qualifier) => NewColumnId) = this

  override def toString: String = "no_rollup"
}

case class CompoundRollup(position: Position) extends Hint[Nothing, Nothing] {
  def typ = None

  def mapColumnIds[NewColumnId](f: (Nothing, Qualifier) => NewColumnId) = this

  override def toString: String = "compound_rollup"
}

case class RollupAtJoin(position: Position) extends Hint[Nothing, Nothing] {
  def typ = None

  def mapColumnIds[NewColumnId](f: (Nothing, Qualifier) => NewColumnId) = this

  override def toString: String = "rollup_at_join"
}

case class NoChainMerge(position: Position) extends Hint[Nothing, Nothing] {
  def typ = None

  def mapColumnIds[NewColumnId](f: (Nothing, Qualifier) => NewColumnId) = this

  override def toString: String = "no_chain_merge"
}