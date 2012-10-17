package com.socrata.soql.analysis

import com.socrata.soql.ast._
import com.socrata.soql.names._
import util.parsing.input.Position
import com.socrata.collection.OrderedSet

abstract class Typechecker[Type] { self =>
  def aliases: Map[ColumnName, typed.TypedFF[Type]]
  def columns: Map[ColumnName, Type]

  val functionCallTypechecker = new FunctionCallTypechecker[Type] {
    def typeParameterUniverse = self.typeParameterUniverse

    def implicitConversions(from: Type, to: Type) = self.implicitConversions(from, to)

    def canBePassedToWithoutConversion(actual: Type, expected: Type) = self.canBePassedToWithoutConversion(actual, expected)
  }

  def apply(e: Expression) = typecheck(e.removeParens)

  private def typecheck(e: Expression): typed.TypedFF[Type] = e match {
    case ColumnOrAliasRef(col) =>
      aliases.get(col) match {
        case Some(tree) =>
          tree
        case None =>
          columns.get(col) match {
            case Some(typ) =>
              p(typed.ColumnRef(col, typ), e)
            case None =>
              throw new UnknownColumn(e.position)
          }
      }
    case c@Cast(expr, targetTypeName) =>
      val typedExpr = typecheck(expr)
      val targetType = typeFor(targetTypeName, c.targetTypePosition)
      val cast = getCastFunction(typedExpr.typ, targetType, c.targetTypePosition)
      typed.FunctionCall(cast, Seq(typedExpr))
    case FunctionCall(name, parameters) =>
      val typedParameters = parameters.map(typecheck(_))

      val options = functionsWithArity(name, typedParameters.length, e.position)
      functionCallTypechecker.resolveOverload(options, typedParameters) match {
        case Matched(f, cs) =>
          val realParameterList = (typedParameters, cs).zipped.map { (param, converter) =>
            converter match {
              case None => param
              case Some(conv) => typed.FunctionCall(conv, Seq(param))
            }
          }
          typed.FunctionCall(f, realParameterList.toSeq)
        case Ambiguous(_) | NoMatch =>
          // when reporting this, remember to convert special functions back to their syntactic form
          // also TODO: better error reporting in the "no match" case
          error("nyi")
      }
    case BooleanLiteral(b) =>
      typed.BooleanLiteral(b, booleanLiteralType(b))
    case StringLiteral(s) =>
      typed.StringLiteral(s, stringLiteralType(s))
    case NumberLiteral(n) =>
      typed.NumberLiteral(n, numberLiteralType(n))
    case NullLiteral() =>
      typed.NullLiteral(nullLiteralType)
  }

  private def p(tff: typed.TypedFF[Type], uff: Expression) = {
    tff.position = uff.position
    tff
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
