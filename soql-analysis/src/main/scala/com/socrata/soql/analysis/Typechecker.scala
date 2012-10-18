package com.socrata.soql.analysis

import scala.util.parsing.input.{NoPosition, Position}

import com.socrata.soql.ast._
import com.socrata.soql.names._
import com.socrata.collection.OrderedSet

class TypeMismatchError[Type](val name: FunctionName, val actual: Type, position: Position) extends Exception("Cannot pass a value of type " + actual + " to " + name + ":\n" + position.longString)
class AmbiguousCall[Type](val name: FunctionName, position: Position) extends Exception("Ambiguous call to " + name + ":\n" + position.longString)

abstract class Typechecker[Type] extends (Expression => typed.TypedFF[Type]) { self =>
  def aliases: Map[ColumnName, typed.TypedFF[Type]]
  def columns: Map[ColumnName, Type]

  val functionCallTypechecker = new FunctionCallTypechecker[Type] {
    def typeParameterUniverse = self.typeParameterUniverse

    def implicitConversions(from: Type, to: Type) = self.implicitConversions(from, to)

    def canBePassedToWithoutConversion(actual: Type, expected: Type) = self.canBePassedToWithoutConversion(actual, expected)
  }

  def apply(e: Expression) = typecheck(e.removeParens)

  private def typecheck(e: Expression): typed.TypedFF[Type] = e match {
    case r@ColumnOrAliasRef(col) =>
      aliases.get(col) match {
        case Some(tree) =>
          tree
        case None =>
          columns.get(col) match {
            case Some(typ) =>
              typed.ColumnRef(col, typ).positionedAt(r.position)
            case None =>
              throw new UnknownColumn(r.position)
          }
      }
    case c@Cast(expr, targetTypeName) =>
      val typedExpr = typecheck(expr)
      val targetType = typeFor(targetTypeName, c.targetTypePosition)
      val cast = getCastFunction(typedExpr.typ, targetType, c.targetTypePosition)
      typed.FunctionCall(cast, Seq(typedExpr)).positionedAt(c.position).functionNameAt(c.operatorPosition)
    case fc@FunctionCall(name, parameters) =>
      val typedParameters = parameters.map(typecheck(_))

      val options = functionsWithArity(name, typedParameters.length, fc.functionNamePosition)
      functionCallTypechecker.resolveOverload(options, typedParameters) match {
        case Matched(f, cs) =>
          val realParameterList = (typedParameters, cs).zipped.map { (param, converter) =>
            converter match {
              case None => param
              case Some(conv) => typed.FunctionCall(conv, Seq(param)).positionedAt(NoPosition).functionNameAt(NoPosition)
            }
          }
          typed.FunctionCall(f, realParameterList.toSeq).positionedAt(fc.position).functionNameAt(fc.functionNamePosition)
        case NoMatch =>
          val failure = functionCallTypechecker.narrowDownFailure(options, typedParameters)
          throw new TypeMismatchError(name, typedParameters(failure.idx).typ, typedParameters(failure.idx).position)
        case Ambiguous(_) | NoMatch =>
          // when reporting this, remember to convert special functions back to their syntactic form
          // also TODO: better error reporting in the "no match" case
          throw new AmbiguousCall(name, fc.functionNamePosition)
      }
    case bl@BooleanLiteral(b) =>
      typed.BooleanLiteral(b, booleanLiteralType(b)).positionedAt(bl.position)
    case sl@StringLiteral(s) =>
      typed.StringLiteral(s, stringLiteralType(s)).positionedAt(sl.position)
    case nl@NumberLiteral(n) =>
      typed.NumberLiteral(n, numberLiteralType(n)).positionedAt(nl.position)
    case nl@NullLiteral() =>
      typed.NullLiteral(nullLiteralType).positionedAt(nl.position)
  }

  def booleanLiteralType(b: Boolean): Type
  def stringLiteralType(s: String): Type
  def numberLiteralType(n: BigDecimal): Type
  def nullLiteralType: Type

  def isAggregate(function: MonomorphicFunction[Type]): Boolean

  // throws NoSuchFunction exception
  def functionsWithArity(name: FunctionName, n: Int, position: Position): Set[Function[Type]]

  // throws UnknownType exception
  def typeFor(name: TypeName, position: Position): Type

  // throws IncompatibleType exception
  def getCastFunction(from: Type, to: Type, position: Position): MonomorphicFunction[Type]

  def typeParameterUniverse: OrderedSet[Type]

  def implicitConversions(from: Type, to: Type): Option[MonomorphicFunction[Type]]

  def canBePassedToWithoutConversion(actual: Type, expected: Type): Boolean
}
