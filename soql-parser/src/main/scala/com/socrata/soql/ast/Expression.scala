package com.socrata.soql.ast

import scala.util.parsing.input.{Position, NoPosition}
import scala.runtime.ScalaRunTime

import com.socrata.soql.names.{FunctionName, ColumnName, TypeName}

sealed abstract class Expression extends Product {
  var position: Position = NoPosition
  protected def asString: String
  override final def toString = if(Expression.pretty) asString else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)
  def allColumnRefs: Set[ColumnOrAliasRef]

  def positionedAt(p: Position): this.type = {
    position = p
    this
  }

  def toSyntheticIdentifierBase: String = {
    // This is clearly not optimized, but the inputs are small and it
    // mirrors the prose spec closely.

    import Expression._
    val SyntheticUnderscore = Int.MaxValue
    val StartOfString = Int.MaxValue - 1
    val EndOfString = Int.MaxValue - 2

    // First find all tokens to keep and replace "bad" characters with synthetic underscores
    val good_chars_only = findIdentsAndLiterals(this).map(replaceBadChars(_, SyntheticUnderscore))
    // Join them up with synthetic underscores
    val separated_by_underscores = joinWith(good_chars_only, SyntheticUnderscore)
    // Collapse runs of adjacent synthetic underscores
    val not_so_many = collapseRuns(separated_by_underscores, SyntheticUnderscore)
    // Remove ones that are next to real underscores or the ends
    val not_next_to = removeAdjacent(StartOfString +: not_so_many :+ EndOfString, SyntheticUnderscore, Set('_'.toInt, '-'.toInt, StartOfString, EndOfString))
    // Remove the start/end markers
    val trimmed = not_next_to.slice(1, not_next_to.length - 1)
    // Convert synthetic underscores to real ones
    val asString: String = trimmed.map {
      case SyntheticUnderscore => '_'
      case other => other.toChar
    } (scala.collection.breakOut)
    // make sure the result is a valid identifier and return it
    if(asString.isEmpty) "_"
    else if(!Character.isJavaIdentifierStart(asString.charAt(0)) && asString.charAt(0) != '-') "_" + asString
    else asString
  }
}

object Expression {
  val pretty = AST.pretty

  private def collapseRuns[T](in: Seq[T], v: T): Seq[T] = {
    val r = new scala.collection.immutable.VectorBuilder[T]
    val it = in.iterator.buffered
    while(it.hasNext) {
      val here = it.next()
      if(here == v) {
        while(it.hasNext && it.head == v) it.next()
      }
      r += here
    }
    r.result
  }

  private def removeAdjacent[T](in: Seq[T], toRemove: T, ifAdjacentToThese: Set[T]): Seq[T] = {
    val r = new scala.collection.immutable.VectorBuilder[T]
    val it = in.iterator.buffered
    var lastWasAdjacentTarget = false
    while(it.hasNext) {
      val here = it.next()
      if(lastWasAdjacentTarget && here == toRemove) {
        // do nothing; lastWasAdjacentTarget will remain true
      } else {
        if(here == toRemove && it.hasNext && ifAdjacentToThese(it.head)) {
          // still do nothing because the next thing is the neighbor
        } else {
          lastWasAdjacentTarget = ifAdjacentToThese(here)
          r += here
        }
      }
    }
    r.result
  }

  private def replaceBadChars(s: String, replacement: Int): IndexedSeq[Int] = {
    s.map {
      case c if Character.isJavaIdentifierPart(c) => c
      case c@'-' => c
      case _ => replacement
    }
  }

  private def findIdentsAndLiterals(e: Expression): Seq[String] = e match {
    case v: Literal => Vector(v.asString)
    case ColumnOrAliasRef(name) => Vector(name.canonicalName)
    case fc: FunctionCall =>
      fc match {
        case FunctionCall(SpecialFunctions.StarFunc(base), Seq()) => Vector(base)
        case FunctionCall(SpecialFunctions.Operator("-"), args) => args.flatMap(findIdentsAndLiterals) // otherwise minus looks like a non-synthetic underscore
        case FunctionCall(SpecialFunctions.Operator(op), Seq(arg)) => op +: findIdentsAndLiterals(arg)
        case FunctionCall(SpecialFunctions.Operator(op), Seq(arg1, arg2)) => findIdentsAndLiterals(arg1) ++ Vector(op) ++ findIdentsAndLiterals(arg2)
        case FunctionCall(SpecialFunctions.Operator(_), _) => sys.error("Found a non-unary, non-binary operator: " + fc)
        case FunctionCall(SpecialFunctions.Cast(typ), Seq(arg)) => findIdentsAndLiterals(arg) :+ typ.canonicalName
        case FunctionCall(SpecialFunctions.Cast(_), _) => sys.error("Found a non-unary cast: " + fc)
        case FunctionCall(SpecialFunctions.IsNull, args) => args.flatMap(findIdentsAndLiterals) ++ Vector("is", "null")
        case FunctionCall(SpecialFunctions.IsNotNull, args) => args.flatMap(findIdentsAndLiterals) ++ Vector("is", "not", "null")
        case FunctionCall(SpecialFunctions.Between, Seq(a,b,c)) =>
          findIdentsAndLiterals(a) ++ Vector("between") ++ findIdentsAndLiterals(b) ++ Vector("and") ++ findIdentsAndLiterals(c)
        case FunctionCall(SpecialFunctions.NotBetween, Seq(a,b,c)) =>
          findIdentsAndLiterals(a) ++ Vector("not", "between") ++ findIdentsAndLiterals(b) ++ Vector("and") ++ findIdentsAndLiterals(c)
        case FunctionCall(other, args) => Vector(other.canonicalName) ++ args.flatMap(findIdentsAndLiterals)
      }
  }

  private def joinWith[T](xs: Seq[Seq[T]], i: T): Seq[T] = {
    val r = new scala.collection.immutable.VectorBuilder[T]
    val it = xs.iterator
    while(it.hasNext) {
      r ++= it.next()
      if(it.hasNext) r += i
    }
    r.result
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

  // both of these are redundant but needed for synthetic identifiers because we need to
  // distinguish between "not (x is null)" and "x is not null" when generating them.
  val IsNotNull = FunctionName("#IS_NOT_NULL")
  val NotBetween = FunctionName("#NOT_BETWEEN")
  val In = FunctionName("#IN")
  val NotIn = FunctionName("#NOT_IN")

  object Operator {
    def apply(op: String) = FunctionName("op$" + op)
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(x)
      case _ => None
    }
    val Regex = """^op\$(.*)$""".r
  }
  val Subscript = Operator("[]")

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

case class ColumnOrAliasRef(column: ColumnName) extends Expression {
  protected def asString = column.toString
  def allColumnRefs = Set(this)
}

sealed abstract class Literal extends Expression {
  def allColumnRefs = Set.empty
}
case class NumberLiteral(value: BigDecimal) extends Literal {
  protected def asString = value.toString
}
case class StringLiteral(value: String) extends Literal {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
}
case class BooleanLiteral(value: Boolean) extends Literal {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral() extends Literal {
  override final def asString = "null"
}

case class FunctionCall(functionName: FunctionName, parameters: Seq[Expression]) extends Expression {
  var functionNamePosition: Position = NoPosition
  protected def asString = functionName match {
    case SpecialFunctions.Parens => "(" + parameters(0) + ")"
    case SpecialFunctions.Subscript => parameters(0) + "[" + parameters(1) + "]"
    case SpecialFunctions.StarFunc(f) => f + "(*)"
    case SpecialFunctions.Operator(op) if parameters.size == 1 => op + parameters(0)
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
    case other => parameters.mkString(other + "(", ",", ")")
  }
  lazy val allColumnRefs = parameters.foldLeft(Set.empty[ColumnOrAliasRef])(_ ++ _.allColumnRefs)

  def functionNameAt(p: Position): this.type = {
    functionNamePosition = p
    this
  }
}
