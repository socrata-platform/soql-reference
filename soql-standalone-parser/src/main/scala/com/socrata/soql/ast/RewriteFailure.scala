package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}

import com.socrata.soql.environment._

sealed abstract class RewriteFailure extends Exception {
  val position: Position
}
case class UnknownUDF(name: TableName)(val position: Position) extends RewriteFailure
case class MismatchedParameterCount(expected: Int, got: Int)(val position: Position) extends RewriteFailure
