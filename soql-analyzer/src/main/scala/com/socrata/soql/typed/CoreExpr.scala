package com.socrata.soql.typed

import com.socrata.soql.ast.Expression

import scala.util.parsing.input.Position
import scala.runtime.ScalaRunTime

import com.socrata.soql.functions.MonomorphicFunction

/** A "core expression" -- nothing but literals, column references, and function calls,
  * with aliases fully expanded and with types ascribed at each node. */
sealed abstract class
CoreExpr[+ColumnId, Type] extends Product with Typable[Type] {
  val position: Position
  val size: Int
  protected def asString: String
  override final def toString = if(CoreExpr.pretty) (asString + " :: " + typ) else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId): CoreExpr[NewColumnId, Type]
}

object CoreExpr {
  val pretty = true
}

case class ColumnRef[ColumnId, Type](column: ColumnId, typ: Type)(val position: Position) extends CoreExpr[ColumnId, Type] {
  protected def asString = column.toString
  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) = copy(column = f(column))(position)
  val size = 0
}

sealed abstract class TypedLiteral[Type] extends CoreExpr[Nothing, Type] {
  def mapColumnIds[NewColumnId](f: Nothing => NewColumnId) = this
  val size = 0
}
case class NumberLiteral[Type](value: BigDecimal, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString
}
case class StringLiteral[Type](value: String, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = Expression.escapeString(value)
}
case class BooleanLiteral[Type](value: Boolean, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral[Type](typ: Type)(val position: Position) extends TypedLiteral[Type] {
  override final def asString = "NULL"
}
case class FunctionCall[ColumnId, Type](function: MonomorphicFunction[Type], parameters: Seq[CoreExpr[ColumnId, Type]])(val position: Position, val functionNamePosition: Position) extends CoreExpr[ColumnId, Type] {
  if(function.isVariadic) {
    require(parameters.length >= function.minArity, "parameter/arity mismatch")
  } else {
    require(parameters.length == function.minArity, "parameter/arity mismatch")
  }

  // require((function.parameters, parameters).zipped.forall { (formal, actual) => formal == actual.typ })
  protected def asString = parameters.mkString(function.name.toString + "(", ",", ")")
  def typ = function.result

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) = copy(parameters = parameters.map(_.mapColumnIds(f)))(position, functionNamePosition)
  val size = parameters.foldLeft(1) { (acc, param) => acc + param.size }
}
