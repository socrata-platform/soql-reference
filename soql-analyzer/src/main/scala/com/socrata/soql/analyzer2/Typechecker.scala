package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, HoleName, ResourceName, TableName, FunctionName}
import com.socrata.soql.typechecker.{TypeInfo, FunctionInfo, FunctionCallTypechecker, Passed, TypeMismatchFailure, HasType}

class Typechecker[CT, CV](
  env: Environment[CT],
  namedExprs: Map[ColumnName, Expr[CT, CV]],
  udfParams: Map[HoleName, Position => Column[CT]],
  userParameters: Map[ResourceName, Map[HoleName, Either[TypedNull[CT], CV]]],
  canonicalName: Option[ResourceName],
  typeInfo: TypeInfo[CT, CV],
  functionInfo: FunctionInfo[CT]
) {
  private val funcallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(expr: ast.Expression, expectedType: Option[CT]): Expr[CT, CV] =
    check(expr).flatMap(disambiguate(_, expectedType, expr.position)) match {
      case Right(expr) => expr
      case Left(err) => throw err
    }

  private def mostPreferredType(types: Set[CT]): CT = {
    typeInfo.typeParameterUniverse.find(types).get
  }

  private def disambiguate(exprs: Seq[Expr[CT, CV]], expectedType: Option[CT], pos: Position): Either[TypecheckError, Expr[CT, CV]] =
    expectedType match {
      case Some(t) =>
        exprs.find(_.typ == t).toRight {
          TypeMismatch(mostPreferredType(exprs.iterator.map(_.typ).toSet), pos)
        }
      case None =>
        Right(exprs.head)
    }

  private def squash(exprs: Seq[Expr[CT, CV]], pos: Position): Either[TypecheckError, Seq[Expr[CT, CV]]] = {
    var acc = OrderedMap.empty[CT, Expr[CT, CV]]

    for { e <- exprs } {
      acc.get(e.typ) match {
        case Some(e2) if e2.size <= e.size =>
          // the smaller (or at least most-preferred) is already chosen
        case Some(_) | None =>
          acc += e.typ -> e
      }
    }

    Right(acc.valuesIterator.toSeq)
  }

  private def check(expr: ast.Expression): Either[TypecheckError, Seq[Expr[CT, CV]]] = {
    expr match {
      case ast.FunctionCall(ast.SpecialFunctions.Parens, Seq(param), None, None) =>
        check(param)
      case fc@ast.FunctionCall(ast.SpecialFunctions.Subscript, Seq(base, ast.StringLiteral(prop)), None, None) =>
        check(base).flatMap { basePossibilities =>
          val asFieldAccesses = basePossibilities.flatMap { basePossibility =>
            val typ = basePossibility.typ
            val fnName = ast.SpecialFunctions.Field(typeInfo.typeNameFor(typ), prop)
            checkFuncall(fc.copy(functionName = fnName, parameters = Seq(base))(fc.position, fc.functionNamePosition)).
              getOrElse(Nil) // ignore errors here, it'll make us fall back to a raw subscript
          }

          val rawSubscript = checkFuncall(fc)
          if(asFieldAccesses.isEmpty) {
            rawSubscript
          } else {
            rawSubscript match {
              case Left(_) => Right(asFieldAccesses)
              case Right(asSubscripts) => squash(asFieldAccesses ++ asSubscripts, fc.position)
            }
          }
        }
      case fc: ast.FunctionCall =>
        checkFuncall(fc).flatMap(squash(_, fc.position))
      case col@ast.ColumnOrAliasRef(None, name) =>
        namedExprs.get(name) match {
          case Some(prechecked) =>
            Right(Seq(prechecked))
          case None =>
            env.lookup(name) match {
              case None => Left(NoSuchColumn(name, col.position))
              case Some(Environment.LookupResult(table, column, typ)) =>
                Right(Seq(Column(table, column, typ)(col.position)))
            }
        }
      case col@ast.ColumnOrAliasRef(Some(qual), name) =>
        env.lookup(ResourceName(qual.substring(TableName.PrefixIndex)), name) match {
          case None => Left(NoSuchColumn(name, col.position))
          case Some(Environment.LookupResult(table, column, typ)) =>
            Right(Seq(Column(table, column, typ)(col.position)))
        }
      case l: ast.Literal =>
        squash(typeInfo.potentialExprs(l), l.position)
      case hole@ast.Hole.UDF(name) =>
        udfParams.get(name) match {
          case Some(expr) => Right(Seq(expr(hole.position)))
          case None => Left(UnknownUDFParameter(name, hole.position))
        }
      case hole@ast.Hole.SavedQuery(name, Some(view)) =>
        userParameter(ResourceName(view), name, hole.position)
      case hole@ast.Hole.SavedQuery(name, None) =>
        canonicalName match {
          case None =>
            Left(UnknownUserParameter(None, name, hole.position))
          case Some(v) =>
            userParameter(v, name, hole.position)
        }
    }
  }

  private def userParameter(view: ResourceName, name: HoleName, position: Position) =
    userParameters.get(view).flatMap(_.get(name)) match {
      case Some(Left(TypedNull(t))) => Right(Seq(NullLiteral(t)(position)))
      case Some(Right(v)) => Right(Seq(LiteralValue(v)(position)(typeInfo.hasType)))
      case None => Left(UnknownUserParameter(Some(view), name, position))
    }

  private def checkFuncall(fc: ast.FunctionCall): Either[TypecheckError, Seq[Expr[CT, CV]]] = {
    val ast.FunctionCall(name, parameters, filter, window) = fc

    val typedParameters = parameters.map(check).map {
      case Left(tm) => return Left(tm)
      case Right(es) => es
    }

    val typedWindow: Option[(Seq[Expr[CT, CV]], Seq[OrderBy[CT, CV]], Seq[Expr[CT, CV]])] = window.map { w =>
      val typedPartitions = w.partitions.map(apply(_, None))
      val typedOrderings = w.orderings.map(ob => OrderBy(apply(ob.expression, Some(typeInfo.boolType)), ob.ascending, ob.nullLast))
      val typedFrames = w.frames.map { x => apply(x, None) }
      (typedPartitions, typedOrderings, typedFrames)
    }

    val typedFilter: Option[Expr[CT, CV]] = filter.map(apply(_, Some(typeInfo.boolType)))

    val options = functionInfo.functionsWithArity(name, typedParameters.length)

    if(options.isEmpty) {
      return Left(NoSuchFunction(name, typedParameters.length, fc.functionNamePosition))
    }

    val (failed, resolved) = divide(funcallTypechecker.resolveOverload(options, typedParameters.map(_.map(_.typ).toSet))) {
      case Passed(f) => Right(f)
      case tm: TypeMismatchFailure[CT] => Left(tm)
    }

    if(resolved.isEmpty) {
      val TypeMismatchFailure(expected, found, idx) = failed.maxBy(_.idx)
      return Left(TypeMismatch(mostPreferredType(found), parameters(idx).position))
    }

    val potentials = resolved.flatMap { f =>
      val skipTypeCheckAfter = typedParameters.size
      val selectedParameters: Iterator[Seq[Expr[CT,CV]]] = Iterator.from(0).zip(f.allParameters.iterator).zip(typedParameters.iterator).map { case ((idx, expected), options) =>
        val choices =
          if(idx < skipTypeCheckAfter) options.filter(_.typ == expected)
          else options.headOption.toSeq // any type is ok for window functions
        if(choices.isEmpty) sys.error("can't happen, we passed typechecking")
        // we can't commit to a choice here.  Because if we decide this is ambiguous, we have to wait to find out
        // later if "f" is eliminated as a contender by typechecking.  It's only an error if f survives
        // typechecking.
        //
        // This means we actually need to preserve all _permutations_ of subtrees.  Fortunately they
        // won't be common -- the number of permutations is related to the number of distinct type variables
        // available to fill; otherwise unification happens and they're forced to be equal.  We don't presently
        // have any functions with more than two distinct type variables.
        choices
      }

      selectedParameters.toVector.foldRight(Seq(List.empty[Expr[CT, CV]])) { (choices, remainingParams) =>
        choices.flatMap { choice => remainingParams.map(choice :: _) }
      }.map { params: Seq[Expr[CT, CV]] =>
        (typedFilter, typedWindow) match {
          case (Some(boolExpr), Some((partitions, orderings, frames))) =>
            val error =
              if(f.isAggregate) {
                NonWindowFunction(fc.functionName, fc.functionNamePosition)
              } else if(f.needsWindow) {
                NonAggregate(fc.functionName, fc.functionNamePosition)
              } else {
                NonWindowFunction(fc.functionName, fc.functionNamePosition)
              }
            return Left(error)
          case (Some(boolExpr), None) =>
            if(!f.isAggregate) {
              return Left(NonAggregate(fc.functionName, fc.functionNamePosition))
            }
            AggregateFunctionCall(f, params, false, Some(boolExpr))(fc.position, fc.functionNamePosition)
          case (None, Some((partitions, orderings, frames))) =>
            if(!f.needsWindow) {
              return Left(NonWindowFunction(fc.functionName, fc.functionNamePosition))
            }
            // TODO: figure out what "frames" actually means
            WindowedFunctionCall(f, params, partitions, orderings, ???, ???, ???, ???)(fc.position, fc.functionNamePosition)
          case (None, None) =>
            if(f.needsWindow) {
              return Left(RequiresWindow(fc.functionName, fc.functionNamePosition))
            }
            if(f.isAggregate) {
              AggregateFunctionCall(f, params, false, None)(fc.position, fc.functionNamePosition)
            } else {
              FunctionCall(f, params)(fc.position, fc.functionNamePosition)
            }
        }
      }
    }.toSeq

    squash(potentials, fc.position)
  }

  private def divide[T, L, R](xs: Iterable[T])(f: T => Either[L, R]): (Seq[L], Seq[R]) = {
    val left = Seq.newBuilder[L]
    val right = Seq.newBuilder[R]
    for(x <- xs) {
      f(x) match {
        case Left(l) => left += l
        case Right(r) => right += r
      }
    }
    (left.result(), right.result())
  }

  sealed abstract class TypecheckError extends Exception
  case class NoSuchColumn(name: ColumnName, pos: Position) extends TypecheckError
  case class UnknownUDFParameter(name: HoleName, pos: Position) extends TypecheckError
  case class UnknownUserParameter(view: Option[ResourceName], name: HoleName, pos: Position) extends TypecheckError
  private def unqualifiedUserParam(name: HoleName, pos: Position): Nothing = ???
  case class NoSuchFunction(name: FunctionName, arity: Int, pos: Position) extends TypecheckError
  case class TypeMismatch(found: CT, pos: Position) extends TypecheckError
  case class RequiresWindow(name: FunctionName, pos: Position) extends TypecheckError
  case class NonAggregate(name: FunctionName, pos: Position) extends TypecheckError
  case class NonWindowFunction(name: FunctionName, pos: Position) extends TypecheckError
}
