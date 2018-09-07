package com.socrata.soql.ast

import scala.util.parsing.input.Position
import scala.runtime.ScalaRunTime
import com.socrata.soql.environment.{ColumnName, FunctionName, TableName, TypeName}

sealed abstract class Expression extends Product {
  val position: Position
  protected def asString: String
  override final def toString = if(Expression.pretty) asString else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)
  def allColumnRefs: Set[ColumnOrAliasRef]

  def toSyntheticIdentifierBase: String =
    com.socrata.soql.brita.IdentifierFilter(Expression.findIdentsAndLiterals(this))
}

object Expression {
  val pretty = AST.pretty

  def escapeString(s: String): String = "'" + s.replaceAll("'", "''") + "'"

  private def findIdentsAndLiterals(e: Expression): Seq[String] = e match {
    case v: Literal => Vector(v.asString)
    case ColumnOrAliasRef(_, name) => Vector(name.name)
    case fc: FunctionCall =>
      fc match {
        case FunctionCall(SpecialFunctions.StarFunc(base), Seq()) => Vector(base)
        case FunctionCall(SpecialFunctions.Operator("-"), args) => args.flatMap(findIdentsAndLiterals) // otherwise minus looks like a non-synthetic underscore
        case FunctionCall(SpecialFunctions.Operator(op), Seq(arg)) => op +: findIdentsAndLiterals(arg)
        case FunctionCall(SpecialFunctions.Operator(op), Seq(arg1, arg2)) => findIdentsAndLiterals(arg1) ++ Vector(op) ++ findIdentsAndLiterals(arg2)
        case FunctionCall(SpecialFunctions.Operator(_), _) => sys.error("Found a non-unary, non-binary operator: " + fc)
        case FunctionCall(SpecialFunctions.Cast(typ), Seq(arg)) => findIdentsAndLiterals(arg) :+ typ.name
        case FunctionCall(SpecialFunctions.Cast(_), _) => sys.error("Found a non-unary cast: " + fc)
        case FunctionCall(SpecialFunctions.IsNull, args) => args.flatMap(findIdentsAndLiterals) ++ Vector("is", "null")
        case FunctionCall(SpecialFunctions.IsNotNull, args) => args.flatMap(findIdentsAndLiterals) ++ Vector("is", "not", "null")
        case FunctionCall(SpecialFunctions.Between, Seq(a,b,c)) =>
          findIdentsAndLiterals(a) ++ Vector("between") ++ findIdentsAndLiterals(b) ++ Vector("and") ++ findIdentsAndLiterals(c)
        case FunctionCall(SpecialFunctions.NotBetween, Seq(a,b,c)) =>
          findIdentsAndLiterals(a) ++ Vector("not", "between") ++ findIdentsAndLiterals(b) ++ Vector("and") ++ findIdentsAndLiterals(c)
        case FunctionCall(SpecialFunctions.WindowFunctionOver, args) =>
          Seq(args.head).flatMap(findIdentsAndLiterals) ++
            Vector("over") ++
            (if (args.tail.isEmpty) Vector.empty else Vector("partition", "by")) ++
            args.tail.flatMap(findIdentsAndLiterals)
        case FunctionCall(other, args) => Vector(other.name) ++ args.flatMap(findIdentsAndLiterals)
      }
  }
}

object SpecialFunctions {
  object StarFunc {
    def apply(f: String) = FunctionName(f + "/*")
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(x)
      case _ => None
    }
    val Regex = """^(.*)/\*$""".r
  }
  val IsNull = FunctionName("#IS_NULL")
  val Between = FunctionName("#BETWEEN")
  val Like = FunctionName("#LIKE")

  // both of these are redundant but needed for synthetic identifiers because we need to
  // distinguish between "not (x is null)" and "x is not null" when generating them.
  val IsNotNull = FunctionName("#IS_NOT_NULL")
  val NotBetween = FunctionName("#NOT_BETWEEN")
  val In = FunctionName("#IN")
  val NotIn = FunctionName("#NOT_IN")
  val NotLike = FunctionName("#NOT_LIKE")

  // window function: aggregatefunction(x) over (partition by a,b,c...)
  val WindowFunctionOver = FunctionName("#WF_OVER")

  object Operator {
    def apply(op: String) = FunctionName("op$" + op)
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(x)
      case _ => None
    }
    val Regex = """^op\$(.*)$""".r
  }
  val Subscript = Operator("[]")

  object Field {
    def apply(typ: TypeName, field: String) = FunctionName("#FIELD$" + typ.name + "." + field)
    def unapply(f: FunctionName) = f.name match {
      case Regex(t, x) ⇒ Some((TypeName(t), x))
      case _ => None
    }
    val Regex = """^#FIELD\$(.*)\.(.*)$""".r
  }

  // this exists only so that selecting "(foo)" is never semi-explicitly aliased.
  // it's stripped out by the typechecker.
  val Parens = Operator("()")

  object Cast {
    def apply(op: TypeName) = FunctionName("cast$" + op.name)
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(TypeName(x))
      case _ => None
    }
    val Regex = """^cast\$(.*)$""".r
  }
}

case class ColumnOrAliasRef(qualifier: Option[String], column: ColumnName)(val position: Position) extends Expression {

  protected def asString = {
    qualifier.map { q =>
      TableName.Prefix +
        q.substring(TableName.SodaFountainTableNamePrefixSubStringIndex) +
        TableName.Field
    }.getOrElse("") + "`" + column.name + "`"
  }

  def allColumnRefs = Set(this)
}

sealed abstract class Literal extends Expression {
  def allColumnRefs = Set.empty
}
case class NumberLiteral(value: BigDecimal)(val position: Position) extends Literal {
  protected def asString = value.toString
}
case class StringLiteral(value: String)(val position: Position) extends Literal {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
}
case class BooleanLiteral(value: Boolean)(val position: Position) extends Literal {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral()(val position: Position) extends Literal {
  override final def asString = "null"
}

case class FunctionCall(functionName: FunctionName, parameters: Seq[Expression])(val position: Position, val functionNamePosition: Position) extends Expression {
  protected def asString = functionName match {
    case SpecialFunctions.Parens => "(" + parameters(0) + ")"
    case SpecialFunctions.Subscript => parameters(0) + "[" + parameters(1) + "]"
    case SpecialFunctions.Field(_, field) => parameters(0) + "." + field
    case SpecialFunctions.StarFunc(f) => f + "(*)"
    case SpecialFunctions.Operator(op) if parameters.size == 1 =>
      op match {
        case "NOT" => "%s %s".format(op, parameters(0))
        case _ => op + parameters(0)
      }
    case SpecialFunctions.Operator(op) if parameters.size == 2 => parameters(0) + " " + op + " " + parameters(1)
    case SpecialFunctions.Operator(op) => sys.error("Found a non-unary, non-binary operator: " + op + " at " + position)
    case SpecialFunctions.Cast(typ) if parameters.size == 1 => parameters(0) + " :: " + typ
    case SpecialFunctions.Cast(_) => sys.error("Found a non-unary cast at " + position)
    case SpecialFunctions.Between => parameters(0) + " BETWEEN " + parameters(1) + " AND " + parameters(2)
    case SpecialFunctions.NotBetween => parameters(0) + " NOT BETWEEN " + parameters(1) + " AND " + parameters(2)
    case SpecialFunctions.IsNull => parameters(0) + " IS NULL"
    case SpecialFunctions.IsNotNull => parameters(0) + " IS NOT NULL"
    case SpecialFunctions.In => parameters.drop(1).mkString(parameters(0) + " IN (", ",", ")")
    case SpecialFunctions.NotIn => parameters.drop(1).mkString(parameters(0) + " NOT IN (", ",", ")")
    case SpecialFunctions.Like => parameters.mkString(" LIKE ")
    case SpecialFunctions.NotLike => parameters.mkString(" NOT LIKE ")
    case SpecialFunctions.WindowFunctionOver =>
      val partitionBy = if (parameters.size > 1) "PARTITION BY " else ""
      parameters.drop(1).mkString(parameters(0) + " OVER (" + partitionBy, ",", ")")
    case other => parameters.mkString(other + "(", ",", ")")
  }
  lazy val allColumnRefs = parameters.foldLeft(Set.empty[ColumnOrAliasRef])(_ ++ _.allColumnRefs)
}
