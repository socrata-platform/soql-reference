package com.socrata.soql.typed

import scala.util.parsing.input.{Position, NoPosition}
import scala.runtime.ScalaRunTime

import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.environment.ColumnName

/** A "core expression" -- nothing but literals, column references, and function calls,
  * with aliases fully expanded and with types ascribed at each node. */
sealed abstract class CoreExpr[+Type] extends Product with Typable[Type] {
  val position: Position
  protected def asString: String
  override final def toString = if(CoreExpr.pretty) (asString + " :: " + typ) else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)
}

object CoreExpr {
  val pretty = true
}

case class ColumnRef[Type](column: ColumnName, typ: Type)(val position: Position) extends CoreExpr[Type] {
  protected def asString = column.toString
}

sealed abstract class TypedLiteral[Type] extends CoreExpr[Type]
case class NumberLiteral[Type](value: BigDecimal, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString
}
case class StringLiteral[Type](value: String, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
}
case class BooleanLiteral[Type](value: Boolean, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral[Type](typ: Type)(val position: Position) extends TypedLiteral[Type] {
  override final def asString = "NULL"
}
case class FunctionCall[Type](function: MonomorphicFunction[Type], parameters: Seq[CoreExpr[Type]])(val position: Position, val functionNamePosition: Position) extends CoreExpr[Type] {
  if(function.isVariadic) {
    require(parameters.length >= function.minArity, "parameter/arity mismatch")
  } else {
    require(parameters.length == function.minArity, "parameter/arity mismatch")
  }

  // require((function.parameters, parameters).zipped.forall { (formal, actual) => formal == actual.typ })
  protected def asString = parameters.mkString(function.name.toString + "(", ",", ")")
  def typ = function.result
}
