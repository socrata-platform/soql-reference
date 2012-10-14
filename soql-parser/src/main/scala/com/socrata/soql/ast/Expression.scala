package com.socrata.soql.ast

import scala.util.parsing.input.{Position, NoPosition}

sealed abstract class Expression extends Product {
  /** The position of the start of this expression in the source code */
  def position: Position
  def withPosition(position: Position): Expression
  override final def toString =
    if(AST.pretty) asString
    else AST.unpretty(this)

  protected def asString: String

  /** Returns a string usable as a base for synthetic column names.  This is
   * NOT sufficient in itself, as it does not check for collisions.  Another
   * stage will, if necessary, append the disambiguation. */
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

  // produces a version of this tree without any position information
  // to make fixtures easier
  def unpositioned: Expression
}

object Expression {
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
    case _: NullLiteral => Vector("null")
    case v: ValueLiteral[_] => Vector(v.value.toString)
    case Identifier(name, _, _) => Vector(name)
    case Paren(expr, _) => findIdentsAndLiterals(expr)
    case FunctionCall(func, NormalFunctionParameters(params), _) =>
      findIdentsAndLiterals(func) ++ params.flatMap(findIdentsAndLiterals)
    case FunctionCall(func, StarParameter(_), _) =>
      findIdentsAndLiterals(func)
    case UnaryOperation(KeywordOperator(op, _), arg, _) =>
      op +: findIdentsAndLiterals(arg)
    case UnaryOperation(SymbolicOperator(_, _), arg, _) =>
      findIdentsAndLiterals(arg)
    case BinaryOperation(KeywordOperator(op, _), a, b, _) =>
      findIdentsAndLiterals(a) ++ Vector(op) ++ findIdentsAndLiterals(b)
    case BinaryOperation(SymbolicOperator(_, _), a, b, _) =>
      findIdentsAndLiterals(a) ++ findIdentsAndLiterals(b)
    case Dereference(expr, field, _) =>
      findIdentsAndLiterals(expr) ++ findIdentsAndLiterals(field)
    case Subscript(expr, index, _) =>
      findIdentsAndLiterals(expr) ++ findIdentsAndLiterals(index)
    case Cast(expr, targetType, _) =>
      findIdentsAndLiterals(expr) ++ findIdentsAndLiterals(targetType)
    case IsNull(expr, negated, _) =>
      val suffix =
        if(negated) Vector("IS", "NOT", "NULL")
        else Vector("IS", "NULL")
      findIdentsAndLiterals(expr) ++ suffix
    case Between(expr, negated, lowerBound, upperBound, _) =>
      val center =
        if(negated) Vector("NOT", "BETWEEN")
        else Vector("BETWEEN")
      findIdentsAndLiterals(expr) ++ center ++ findIdentsAndLiterals(lowerBound) ++ Vector("AND") ++ findIdentsAndLiterals(upperBound)
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

sealed abstract class Literal extends Expression {
  def unpositioned = withPosition(NoPosition)
}
final case class NullLiteral(position: Position) extends Literal {
  protected def asString = "NULL"
  def withPosition(p: Position) = copy(position = p)
}

sealed abstract class ValueLiteral[T] extends Literal {
  def value: T
}

final case class NumberLiteral(value: BigDecimal, position: Position) extends ValueLiteral[BigDecimal] {
  protected def asString = value.toString
  def withPosition(p: Position) = copy(position = p)
}

final case class StringLiteral(value: String, position: Position) extends ValueLiteral[String] {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
  def withPosition(p: Position) = copy(position = p)
}

final case class BooleanLiteral(value: Boolean, position: Position) extends ValueLiteral[Boolean] {
  protected def asString = value.toString
  def withPosition(p: Position) = copy(position = p)
}

final case class Identifier(name: String, quoted: Boolean, position: Position) extends Expression {
  protected def asString =
    if(quoted) "`" + name + "`"
    else name

  def withPosition(p: Position) = copy(position = p)
  def unpositioned = copy(position = NoPosition)
}

final case class Paren(expression: Expression, position: Position) extends Expression { // needed for "simple name" analysis
  protected def asString = "(" + expression + ")"
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = Paren(expression.unpositioned, NoPosition)
}

sealed abstract class FunctionParameters {
  def unpositioned: FunctionParameters
}
case class NormalFunctionParameters(parameters: Seq[Expression]) extends FunctionParameters {
  def unpositioned = NormalFunctionParameters(parameters.map(_.unpositioned))
}
case class StarParameter(position: Position) extends FunctionParameters {
  def unpositioned = StarParameter(NoPosition)
}

final case class FunctionCall(name: Identifier, parameters: FunctionParameters, position: Position) extends Expression {
  protected def asString = parameters match {
    case NormalFunctionParameters(params) => params.mkString(name + "(", ", ", ")")
    case StarParameter(_) => name + "(*)"
  }
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = FunctionCall(name.unpositioned, parameters.unpositioned, NoPosition)
}

sealed abstract class Operator {
  def name: String
  def position: Position
  def unpositioned: Operator
  final override def toString = name
}
final case class SymbolicOperator(name: String, position: Position) extends Operator {
  def unpositioned = copy(position = NoPosition)
}
final case class KeywordOperator(name: String, position: Position) extends Operator {
  def unpositioned = copy(position = NoPosition)
}

final case class UnaryOperation(operator: Operator, argument: Expression, position: Position) extends Expression {
  protected def asString = operator match {
    case SymbolicOperator(opName, _) => opName + argument
    case KeywordOperator(opName, _) => opName + " " + argument
  }
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = UnaryOperation(operator.unpositioned, argument.unpositioned, NoPosition)
}

final case class BinaryOperation(operator: Operator, lhs: Expression, rhs: Expression, position: Position) extends Expression {
  protected def asString = lhs + " " + operator + " " + rhs
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = BinaryOperation(operator.unpositioned, lhs.unpositioned, rhs.unpositioned, NoPosition)
}

final case class Dereference(expression: Expression, field: Identifier, position: Position) extends Expression {
  protected def asString = expression + "." + field
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = Dereference(expression.unpositioned, field.unpositioned, NoPosition)
}

final case class Subscript(expression: Expression, index: Expression, position: Position) extends Expression {
  protected def asString = expression + "[" + index + "]"
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = Subscript(expression.unpositioned, index.unpositioned, NoPosition)
}

final case class Cast(expression: Expression, targetType: Identifier, position: Position) extends Expression {
  protected def asString = expression + " :: " + targetType
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = Cast(expression.unpositioned, targetType.unpositioned, NoPosition)
}

final case class IsNull(expression: Expression, negated: Boolean, position: Position) extends Expression {
  protected def asString = expression + (if(negated) " IS NOT NULL" else " IS NULL")
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = IsNull(expression.unpositioned, negated, NoPosition)
}

final case class Between(expression: Expression, negated: Boolean, lowerBound: Expression, upperBound: Expression, position: Position) extends Expression {
  protected def asString = expression + (if(negated) " NOT BETWEEN " else " BETWEEN ") + lowerBound + " AND " + upperBound
  def withPosition(p: Position) = copy(position = p)
  def unpositioned = Between(expression.unpositioned, negated, lowerBound.unpositioned, upperBound.unpositioned, NoPosition)
}
