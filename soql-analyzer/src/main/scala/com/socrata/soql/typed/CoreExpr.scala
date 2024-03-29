package com.socrata.soql.typed

import scala.util.parsing.input.Position
import scala.runtime.ScalaRunTime

import com.socrata.soql.ast.Expression
import com.socrata.soql.functions.MonomorphicFunction

/** A "core expression" -- nothing but literals, column references, and function calls,
  * with aliases fully expanded and with types ascribed at each node. */
sealed abstract class CoreExpr[+ColumnId, Type] extends Product with Typable[Type] {
  val position: Position
  val size: Int
  protected def asString: String
  override final def toString = if(CoreExpr.pretty) (asString + " :: " + typ) else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId): CoreExpr[NewColumnId, Type]
}

object CoreExpr {
  val pretty = true
}

case class ColumnRef[ColumnId, Type](qualifier: Qualifier, column: ColumnId, typ: Type)(val position: Position) extends CoreExpr[ColumnId, Type] {
  protected def asString = qualifier.map(_ + ".").getOrElse("") + column.toString
  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId) = copy(column = f(column, qualifier))
  val size = 0

  def copy[C, T](qualifier: Qualifier = qualifier,
                 column: C = column,
                 typ: T = typ): ColumnRef[C, T] = ColumnRef[C, T](qualifier, column, typ)(position)
}

sealed abstract class TypedLiteral[Type] extends CoreExpr[Nothing, Type] {
  def mapColumnIds[NewColumnId](f: (Nothing, Qualifier) => NewColumnId) = this

  val size = 0
}

case class NumberLiteral[Type](value: BigDecimal, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString

  def copy[T](value: BigDecimal = value, typ: T = typ): NumberLiteral[T] = NumberLiteral[T](value, typ)(position)
}

case class StringLiteral[Type](value: String, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = Expression.escapeString(value)

  def copy[T](value: String = value, typ: T = typ): StringLiteral[T] = StringLiteral[T](value, typ)(position)
}

case class BooleanLiteral[Type](value: Boolean, typ: Type)(val position: Position) extends TypedLiteral[Type] {
  protected def asString = value.toString.toUpperCase

  def copy[T](value: Boolean = value, typ: T = typ): BooleanLiteral[T] = BooleanLiteral[T](value, typ)(position)
}

case class NullLiteral[Type](typ: Type)(val position: Position) extends TypedLiteral[Type] {
  override final def asString = "NULL"

  def copy[T](typ: T = typ): NullLiteral[T] = NullLiteral[T](typ)(position)
}

case class FunctionCall[ColumnId, Type](function: MonomorphicFunction[Type], parameters: Seq[CoreExpr[ColumnId, Type]],
                                        filter: Option[CoreExpr[ColumnId, Type]],
                                        window: Option[WindowFunctionInfo[ColumnId, Type]])
                                       (val position: Position, val functionNamePosition: Position)
  extends CoreExpr[ColumnId, Type] {

  if(function.isVariadic) {
    require(parameters.length >= function.minArity, "parameter/arity mismatch")
  } else {
    require(parameters.length == function.minArity, "parameter/arity mismatch")
  }

  // require((function.parameters, parameters).zipped.forall { (formal, actual) => formal == actual.typ })
  protected def asString = {
    val seq = parameters.mkString(function.name.toString + "(", ",", ")") +:
      (filter.map(f => s"FILTER(WHERE ${f})").toSeq ++ window.map(_.toString).toSeq)
    seq.mkString(" ")
  }

  def typ = function.result

  def mapColumnIds[NewColumnId](f: (ColumnId, Qualifier) => NewColumnId) = {
    val mw = window.map { w =>
      val mp = w.partitions.map(_.mapColumnIds(f))
      val mo = w.orderings.map(_.mapColumnIds(f))
      val mf = w.frames.map(_.mapColumnIds(f))
      WindowFunctionInfo(mp, mo, mf)
    }

    copy(parameters = parameters.map(_.mapColumnIds(f)), filter = filter.map(_.mapColumnIds(f)), window = mw)
  }

  val size = parameters.foldLeft(1) { (acc, param) => acc + param.size }

  def copy[C, T](function: MonomorphicFunction[T] = function,
                 parameters: Seq[CoreExpr[C, T]] = parameters,
                 filter: Option[CoreExpr[C, T]] = filter,
                 window: Option[WindowFunctionInfo[C, T]] = window): FunctionCall[C, T] =
    FunctionCall(function, parameters, filter, window)(position, functionNamePosition)
}

case class WindowFunctionInfo[ColumnId, Type](partitions: Seq[CoreExpr[ColumnId, Type]],
                                              orderings: Seq[com.socrata.soql.typed.OrderBy[ColumnId, Type]],
                                              frames: Seq[CoreExpr[ColumnId, Type]]) {
}
