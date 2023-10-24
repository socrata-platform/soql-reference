package com.socrata.soql.functions

sealed abstract class FunctionType

object FunctionType {
  case object Normal extends FunctionType
  case object Aggregate extends FunctionType
  case class Window(frameAllowed: Boolean) extends FunctionType
}
