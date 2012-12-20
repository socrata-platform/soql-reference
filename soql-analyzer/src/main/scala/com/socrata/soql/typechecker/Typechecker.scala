package com.socrata.soql.typechecker

import scala.util.parsing.input.NoPosition

import com.socrata.soql.ast._
import com.socrata.soql.exceptions.{NoSuchColumn, NoSuchFunction, TypeMismatch, AmbiguousCall}
import com.socrata.soql.typed
import com.socrata.soql.environment.{ColumnName, DatasetContext}

class Typechecker[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type])(implicit ctx: DatasetContext[Type]) extends ((Expression, Map[ColumnName, typed.CoreExpr[Type]]) => typed.CoreExpr[Type]) { self =>
  import typeInfo._

  type Expr = typed.CoreExpr[Type]

  val columns = ctx.schema
  val functionCallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(e: Expression, aliases: Map[ColumnName, Expr]) = canonicalizeType(typecheck(e, aliases))

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
    case FunctionCall(SpecialFunctions.Parens, params) =>
      assert(params.length == 1, "Parens with more than one parameter?!")
      typecheck(params(0), aliases)
    case fc@FunctionCall(name, parameters) =>
      val typedParameters = parameters.map(typecheck(_, aliases))

      val options = functionInfo.functionsWithArity(name, typedParameters.length)
      if(options.isEmpty) throw NoSuchFunction(name, typedParameters.length, fc.functionNamePosition)
      functionCallTypechecker.resolveOverload(options, typedParameters) match {
        case Matched(f, cs) =>
          val realParameterList = (typedParameters, cs).zipped.map { (param, converter) =>
            converter match {
              case None => param
              case Some(conv) => typed.FunctionCall(conv, Seq(canonicalizeType(param))).positionedAt(NoPosition).functionNameAt(NoPosition)
            }
          }
          typed.FunctionCall(f, realParameterList.toSeq.map(canonicalizeType)).positionedAt(fc.position).functionNameAt(fc.functionNamePosition)
        case NoMatch =>
          val failure = functionCallTypechecker.narrowDownFailure(options, typedParameters)
          throw TypeMismatch(name, typeNameFor(typedParameters(failure.idx).typ), typedParameters(failure.idx).position)
        case Ambiguous(_) =>
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

  def canonicalizeType(expr: Expr): Expr = expr match {
    case fc@typed.FunctionCall(_, _) => fc // a functioncall's type is intrinsic, so it shouldn't need canonicalizing
    case cr@typed.ColumnRef(c, t) => typed.ColumnRef(c, typeInfo.canonicalize(t)).positionedAt(cr.position)
    case bl@typed.BooleanLiteral(b, t) => typed.BooleanLiteral(b, typeInfo.canonicalize(t)).positionedAt(bl.position)
    case nl@typed.NumberLiteral(n, t) => typed.NumberLiteral(n, typeInfo.canonicalize(t)).positionedAt(nl.position)
    case sl@typed.StringLiteral(s, t) => typed.StringLiteral(s, typeInfo.canonicalize(t)).positionedAt(sl.position)
    case nl@typed.NullLiteral(t) => typed.NullLiteral(typeInfo.canonicalize(t)).positionedAt(nl.position)
  }
}
