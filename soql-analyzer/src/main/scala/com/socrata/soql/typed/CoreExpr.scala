package com.socrata.soql.typed

import com.socrata.soql.ast.Expression
import com.socrata.soql.environment.ResourceName

import scala.util.parsing.input.Position
import scala.runtime.ScalaRunTime

import com.socrata.soql.functions.MonomorphicFunction

/** A "core expression" -- nothing but literals, column references, and function calls,
  * with aliases fully expanded and with types ascribed at each node. */
sealed abstract class CoreExpr[+ColumnId, Type] extends Product with Typable[Type] {
  val position: Position
  val size: Int
  protected def asString: String
  override final def toString = if(CoreExpr.pretty) (asString + " :: " + typ) else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId): CoreExpr[NewColumnId, Type]

  def at(newPosition: Position): CoreExpr[ColumnId, Type]
}

object CoreExpr {
  val pretty = true
}

case class ColumnRef[ColumnId, Type](column: ColumnId, typ: Type)(val position: Position) extends CoreExpr[ColumnId, Type] {
  protected def asString = column.toString
  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) = copy(column = f(column))
  val size = 0

  def copy[C, T](column: C = column,
                 typ: T = typ): ColumnRef[C, T] = ColumnRef[C, T](column, typ)(position)

  def at(newPosition: Position) = ColumnRef(column, typ)(newPosition)
}

sealed abstract class TypedLiteral[Type] extends CoreExpr[Nothing, Type] {
  def mapColumnIds[NewColumnId](f: Nothing => NewColumnId) = this

  val size = 0
}

case class NumberLiteral[Type](value: BigDecimal, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString

  def copy[T](value: BigDecimal = value, typ: T = typ): NumberLiteral[T] = NumberLiteral[T](value, typ)(position)
  def at(newPosition: Position) = NumberLiteral(value, typ)(newPosition)
}

case class StringLiteral[Type](value: String, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = Expression.escapeString(value)

  def copy[T](value: String = value, typ: T = typ): StringLiteral[T] = StringLiteral[T](value, typ)(position)
  def at(newPosition: Position) = StringLiteral(value, typ)(newPosition)
}

case class BooleanLiteral[Type](value: Boolean, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString.toUpperCase

  def copy[T](value: Boolean = value, typ: T = typ): BooleanLiteral[T] = BooleanLiteral[T](value, typ)(position)
  def at(newPosition: Position) = BooleanLiteral(value, typ)(newPosition)
}

case class NullLiteral[Type](typ: Type)(val position: Position) extends TypedLiteral[Type] {
  override final def asString = "NULL"

  def copy[T](typ: T = typ): NullLiteral[T] = NullLiteral[T](typ)(position)
  def at(newPosition: Position) = NullLiteral(typ)(newPosition)
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

  def mapColumnIds[NewColumnId](f: ColumnId => NewColumnId) = copy(parameters = parameters.map(_.mapColumnIds(f)))
  val size = parameters.foldLeft(1) { (acc, param) => acc + param.size }

  def copy[C, T](function: MonomorphicFunction[T] = function,
           parameters: Seq[CoreExpr[C, T]] = parameters): FunctionCall[C, T] =
    FunctionCall(function, parameters)(position, functionNamePosition)

  def at(newPosition: Position) = FunctionCall(function, parameters)(newPosition, functionNamePosition)
}
