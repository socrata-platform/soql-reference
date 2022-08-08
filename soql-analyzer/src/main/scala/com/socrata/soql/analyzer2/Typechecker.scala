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
  userParameters: Map[String, Map[HoleName, Either[TypedNull[CT], CV]]],
  typeInfo: TypeInfo[CT, CV],
  functionInfo: FunctionInfo[CT]
) {
  private val funcallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(expr: ast.Expression, expectedType: Option[CT]): Expr[CT, CV] =
    disambiguate(check(expr), expectedType, expr.position)

  private def disambiguate(exprs: Seq[Expr[CT, CV]], expectedType: Option[CT], pos: Position): Expr[CT, CV] =
    expectedType match {
      case Some(t) =>
        exprs.find(_.typ == t).getOrElse {
          typeMismatch(exprs.iterator.map(_.typ).toSet, pos)
        }
      case None =>
        exprs.head
    }

  private def squash(exprs: Seq[Expr[CT, CV]], pos: Position): Seq[Expr[CT, CV]] = {
    exprs.foldLeft(OrderedMap.empty[CT, Expr[CT, CV]]) { (acc, e) =>
      acc.get(e.typ) match {
        case Some(e2) if e2.size < e.size =>
          acc
        case Some(e2) if e2.size == e.size =>
          ambiguousExpression(pos)
        case Some(_) | None =>
          acc + (e.typ -> e)
      }
    }.valuesIterator.toSeq
  }

  private def check(expr: ast.Expression): Seq[Expr[CT, CV]] = {
    expr match {
      case fc: ast.FunctionCall =>
        squash(checkFuncall(fc), fc.position)
      case col@ast.ColumnOrAliasRef(None, name) =>
        namedExprs.get(name) match {
          case Some(prechecked) =>
            Seq(prechecked)
          case None =>
            env.lookup(name) match {
              case None => unknownName(name, col.position)
              case Some(Environment.LookupResult(table, column, typ)) =>
                Seq(Column(table, column, typ)(col.position))
            }
        }
      case col@ast.ColumnOrAliasRef(Some(qual), name) =>
        env.lookup(ResourceName(qual.substring(TableName.PrefixIndex)), name) match {
          case None => unknownName(name, col.position)
          case Some(Environment.LookupResult(table, column, typ)) =>
            Seq(Column(table, column, typ)(col.position))
        }
      case l: ast.Literal =>
        squash(typeInfo.potentialExprs(l), l.position)
      case hole@ast.Hole.UDF(name) =>
        udfParams.get(name) match {
          case Some(expr) => Seq(expr(hole.position))
          case None => unknownUdfParam(name, hole.position)
        }
      case hole@ast.Hole.SavedQuery(name, Some(view)) =>
        userParameters.get(view).flatMap(_.get(name)) match {
          case Some(Left(TypedNull(t))) => Seq(NullLiteral(t)(hole.position))
          case Some(Right(v)) => Seq(LiteralValue(v)(hole.position)(typeInfo.hasType))
          case None => unknownUserParam(view, name, hole.position)
        }
      case hole@ast.Hole.SavedQuery(name, None) =>
        unqualifiedUserParam(name, hole.position)
    }
  }

  private def checkFuncall(fc: ast.FunctionCall): Seq[Expr[CT, CV]] = {
    val ast.FunctionCall(name, parameters, filter, window) = fc

    val typedParameters = parameters.map(check)

    val typedWindow: Option[(Seq[Expr[CT, CV]], Seq[OrderBy[CT, CV]], Seq[Expr[CT, CV]])] = window.map { w =>
      val typedPartitions = w.partitions.map(apply(_, None))
      val typedOrderings = w.orderings.map(ob => OrderBy(apply(ob.expression, Some(typeInfo.boolType)), ob.ascending, ob.nullLast))
      val typedFrames = w.frames.map { x => apply(x, None) }
      (typedPartitions, typedOrderings, typedFrames)
    }

    val typedFilter: Option[Expr[CT, CV]] = filter.map(apply(_, Some(typeInfo.boolType)))

    val options = functionInfo.functionsWithArity(name, typedParameters.length)

    if(options.isEmpty) {
      noSuchFunction(name, typedParameters.length, fc.functionNamePosition)
    }

    val (failed, resolved) = divide(funcallTypechecker.resolveOverload(options, typedParameters.map(_.map(_.typ).toSet))) {
      case Passed(f) => Right(f)
      case tm: TypeMismatchFailure[CT] => Left(tm)
    }

    if(resolved.isEmpty) {
      val TypeMismatchFailure(expected, found, idx) = failed.maxBy(_.idx)
      typeMismatch(found, parameters(idx).position)
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
            if(f.isAggregate) nonWindowFunction(fc.functionNamePosition)
            if(f.needsWindow) nonAggregate(fc.functionNamePosition)
            nonWindowFunction(fc.functionNamePosition)
          case (Some(boolExpr), None) =>
            if(!f.isAggregate) nonAggregate(fc.functionNamePosition)
            AggregateFunctionCall(f, params, false, Some(boolExpr))(fc.position, fc.functionNamePosition)
          case (None, Some((partitions, orderings, frames))) =>
            if(!f.needsWindow) nonWindowFunction(fc.functionNamePosition)
            // TODO: figure out what "frames" actually means
            WindowedFunctionCall(f, params, partitions, orderings, ???, ???, ???, ???)(fc.position, fc.functionNamePosition)
          case (None, None) =>
            if(f.needsWindow) requiresWindow(fc.functionNamePosition)
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

  private def unknownName(name: ColumnName, pos: Position): Nothing = ???
  private def unknownUdfParam(name: HoleName, pos: Position): Nothing = ???
  private def unknownUserParam(view: String, name: HoleName, pos: Position): Nothing = ???
  private def unqualifiedUserParam(name: HoleName, pos: Position): Nothing = ???
  private def ambiguousExpression(pos: Position): Nothing = ???
  private def noSuchFunction(name: FunctionName, arity: Int, pos: Position): Nothing = ???
  private def typeMismatch(found: Set[CT], pos: Position): Nothing = ???
  private def requiresWindow(pos: Position): Nothing = ???
  private def nonAggregate(pos: Position): Nothing = ???
  private def nonWindowFunction(pos: Position): Nothing = ???
}
