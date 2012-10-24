package com.socrata.soql.typechecker

import scala.util.parsing.input.NoPosition

import com.socrata.soql.ast._
import com.socrata.soql.exceptions.{NoSuchColumn, UnknownType, ImpossibleCast, NoSuchFunction, TypeMismatch, AmbiguousCall}
import com.socrata.soql.names._
import com.socrata.soql.{DatasetContext, typed}

class Typechecker[Type](typeInfo: TypeInfo[Type])(implicit ctx: DatasetContext[Type]) extends ((Expression, Map[ColumnName, typed.TypedFF[Type]]) => typed.TypedFF[Type]) { self =>
  import typeInfo._

  type Expr = typed.TypedFF[Type]

  val columns = ctx.schema

  val functionCallTypechecker = new FunctionCallTypechecker(typeInfo)

  def apply(e: Expression, aliases: Map[ColumnName, Expr]) = typecheck(e, aliases)

  private def typecheck(e: Expression, aliases: Map[ColumnName, Expr]): Expr = e match {
    case r@ColumnOrAliasRef(col) =>
      aliases.get(col) match {
        case Some(typed.ColumnRef(name, typ)) if name == col =>
          // special case: if this is an alias that refers directly to itself, position the typed tree _here_
          // (as if there were no alias at all) to make error messages that much clearer.  This will only catch
          // semi-implicitly assigned aliases, so it's better anyway.
          typed.ColumnRef(col, typ).positionedAt(r.position)
        case Some(tree) =>
          tree
        case None =>
          columns.get(col) match {
            case Some(typ) =>
              typed.ColumnRef(col, typ).positionedAt(r.position)
            case None =>
              throw NoSuchColumn(col, r.position)
          }
      }
    case c@Cast(expr, targetTypeName) =>
      val typedExpr = typecheck(expr, aliases)
      val targetType = typeFor(targetTypeName).getOrElse {
        throw UnknownType(targetTypeName, c.targetTypePosition)
      }
      val cast = getCastFunction(typedExpr.typ, targetType).getOrElse {
        throw ImpossibleCast(typeNameFor(typedExpr.typ), typeNameFor(targetType), c.targetTypePosition)
      }
      typed.FunctionCall(cast, Seq(typedExpr)).positionedAt(c.position).functionNameAt(c.operatorPosition)
    case FunctionCall(SpecialFunctions.Parens, params) =>
      assert(params.length == 1, "Parens with more than one parameter?!")
      typecheck(params(0), aliases)
    case fc@FunctionCall(name, parameters) =>
      val typedParameters = parameters.map(typecheck(_, aliases))

      val options = functionsWithArity(name, typedParameters.length)
      if(options.isEmpty) throw NoSuchFunction(name, typedParameters.length, fc.functionNamePosition)
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
          throw TypeMismatch(name, typeNameFor(typedParameters(failure.idx).typ), typedParameters(failure.idx).position)
        case Ambiguous(_) | NoMatch =>
          // when reporting this, remember to convert special functions back to their syntactic form
          // also TODO: better error reporting in the "no match" case
          throw AmbiguousCall(name, fc.functionNamePosition)
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
}
