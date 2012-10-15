package com.socrata.soql.analysis.untyped

import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.ast

import com.socrata.soql.ColumnName
import com.socrata.soql.FunctionName
import com.socrata.soql.TypeName
import com.socrata.soql.DatasetContext

/** Untyped Function Form -- like function normal form, but with
 * aliases unexpanded. */
sealed abstract class UntypedFF extends Product {
  var position: Position = NoPosition
  protected def asString: String
  override final def toString = if(UntypedFF.pretty) asString else productIterator.mkString(productPrefix + "(",",",")")
}

object UntypedFF {
  def main(args: Array[String]) {
    implicit val ctx = new DatasetContext {
      val locale = com.ibm.icu.util.ULocale.US
      val columns = Set.empty[ColumnName]
    }
    val p = new com.socrata.soql.parsing.Parser
    p.expression("2 + x::number between 5 and f.latitude") match {
      case p.Success(e, _) =>
        println(apply(e))
    }
  }

  val pretty = false

  def apply(expression: ast.Expression)(implicit ctx: DatasetContext): UntypedFF = expression match {
    case ast.NullLiteral(p) =>
      pos(NullLiteral(), p)
    case ast.NumberLiteral(v, p) =>
      pos(NumberLiteral(v), p)
    case ast.StringLiteral(v, p) =>
      pos(StringLiteral(v), p)
    case ast.BooleanLiteral(v, p) =>
      pos(BooleanLiteral(v), p)
    case ast.Identifier(name, _, p) =>
      pos(ColumnOrAliasRef(ColumnName(name)), p)
    case ast.Paren(e, _) =>
      apply(e)
    case ast.FunctionCall(name, ast.NormalFunctionParameters(params), p) =>
      pos(FunctionCall(FunctionName(name.name), params.map(apply)), p)
    case ast.FunctionCall(name, ast.StarParameter(_), p) =>
      pos(FunctionCall(SpecialFunctions.StarFunc, Seq(pos(StringLiteral(name.name), name.position))), p)
    case ast.UnaryOperation(op, arg, p) =>
      pos(FunctionCall(opName(op), Seq(apply(arg))), p)
    case ast.BinaryOperation(op, lhs, rhs, p) =>
      pos(FunctionCall(opName(op), Seq(apply(lhs), apply(rhs))), p)
    case ast.Dereference(expr, field, p) =>
      pos(FunctionCall(SpecialFunctions.Subscript, Seq(apply(expr), pos(StringLiteral(field.name), field.position))), p)
    case ast.Subscript(expr, index, p) =>
      pos(FunctionCall(SpecialFunctions.Subscript, Seq(apply(expr), apply(index))), p)
    case ast.Cast(expr, targetType, p) =>
      pos(Cast(apply(expr), TypeName(targetType.name)), p)
    case ast.IsNull(expr, false, p) =>
      pos(FunctionCall(SpecialFunctions.IsNull, Seq(apply(expr))), p)
    case ast.IsNull(expr, true, p) =>
      negate(pos(FunctionCall(SpecialFunctions.IsNull, Seq(apply(expr))), p), p)
    case ast.Between(expr, false, a, b, p) =>
      pos(FunctionCall(SpecialFunctions.Between, Seq(apply(expr), apply(a), apply(b))), p)
    case ast.Between(expr, true, a, b, p) =>
      negate(pos(FunctionCall(SpecialFunctions.Between, Seq(apply(expr), apply(a), apply(b))), p), p)
  }

  private def negate(expr: UntypedFF, p: Position) = pos(FunctionCall(SpecialFunctions.NOT, Seq(expr)), p)

  private def opName(op: ast.Operator) = op match {
    case ast.SymbolicOperator(n, _) => FunctionName(n)
    case ast.KeywordOperator(n, _) => FunctionName("#" + n)
  }

  private def pos[T <: UntypedFF](ff: T, p: Position): T = {
    ff.position = p
    ff
  }
}

object SpecialFunctions {
  val StarFunc = FunctionName("F_*")
  val Subscript = FunctionName("[]")
  val IsNull = FunctionName("#IS_NULL")
  val Between = FunctionName("#BETWEEN")
  val NOT = FunctionName(com.socrata.soql.tokens.AND().printable)
}

case class ColumnOrAliasRef(column: ColumnName) extends UntypedFF {
  protected def asString = column.toString
}

sealed abstract class UntypedLiteral extends UntypedFF
case class NumberLiteral(value: BigDecimal) extends UntypedLiteral {
  protected def asString = value.toString
}
case class StringLiteral(value: String) extends UntypedLiteral {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
}
case class BooleanLiteral(value: Boolean) extends UntypedLiteral {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral() extends UntypedLiteral {
  override final def asString = "NULL"
}

case class FunctionCall(function: FunctionName, parameters: Seq[UntypedFF]) extends UntypedFF {
  protected def asString = parameters.mkString(function.toString + "(", ",", ")")
}
case class Cast(expression: UntypedFF, targetType: TypeName) extends UntypedFF {
  protected def asString = "::("+targetType+")"
}
