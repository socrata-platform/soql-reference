package com.socrata.soql.typed

import scala.util.parsing.input.{Position, NoPosition}
import scala.runtime.ScalaRunTime

import com.socrata.soql.names.ColumnName
import com.socrata.soql.functions.MonomorphicFunction

/** Typed Function Form -- fully expanded and with types ascripted at each node. */
sealed abstract class TypedFF[+Type] extends Product with Typable[Type] {
  var position: Position = NoPosition
  protected def asString: String
  override final def toString = if(TypedFF.pretty) (asString + " :: " + typ) else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)

  def positionedAt(p: Position): this.type = {
    position = p
    this
  }
}

object TypedFF {
  val pretty = true
}

case class ColumnRef[Type](column: ColumnName, typ: Type) extends TypedFF[Type] {
  protected def asString = column.toString
}

sealed abstract class TypedLiteral[Type] extends TypedFF[Type]
case class NumberLiteral[Type](value: BigDecimal, typ: Type) extends TypedLiteral[Type] {
  protected def asString = value.toString
}
case class StringLiteral[Type](value: String, typ: Type) extends TypedLiteral[Type] {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
}
case class BooleanLiteral[Type](value: Boolean, typ: Type) extends TypedLiteral[Type] {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral[Type](typ: Type) extends TypedLiteral[Type] {
  override final def asString = "NULL"
}
case class FunctionCall[Type](function: MonomorphicFunction[Type], parameters: Seq[TypedFF[Type]]) extends TypedFF[Type] {
  var functionNamePosition: Position = NoPosition
  require(parameters.length == function.arity, "parameter/arity mismatch")
  // require((function.parameters, parameters).zipped.forall { (formal, actual) => formal == actual.typ })
  protected def asString = parameters.mkString(function.name.toString + "(", ",", ")")
  def typ = function.result

  def functionNameAt(position: Position): this.type = {
    functionNamePosition = position
    this
  }
}
