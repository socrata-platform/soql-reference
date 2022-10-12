package com.socrata.soql.analyzer2

import scala.util.control.NoStackTrace
import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.{OrderedMap, CovariantSet}
import com.socrata.soql.environment.{ColumnName, HoleName, ResourceName, TableName, FunctionName}
import com.socrata.soql.typechecker.{TypeInfo2, FunctionInfo, FunctionCallTypechecker, Passed, TypeMismatchFailure, HasType}

class Typechecker[RNS, CT, CV](
  scope: RNS,
  canonicalName: Option[CanonicalName],
  env: Environment[CT],
  namedExprs: Map[ColumnName, Expr[CT, CV]],
  udfParams: Map[HoleName, Position => Expr[CT, CV]],
  userParameters: UserParameters[CT, CV],
  typeInfo: TypeInfo2[CT, CV],
  functionInfo: FunctionInfo[CT]
) {
  type TypecheckError = SoQLAnalyzerError.TypecheckError[RNS]
  private val TypecheckError = SoQLAnalyzerError.TypecheckError
  import TypecheckError._

  private val funcallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(expr: ast.Expression, expectedType: Option[CT]): Either[TypecheckError, Expr[CT, CV]] =
    try {
      Right(finalCheck(expr, expectedType))
    } catch {
      case Bail(e) =>
        Left(e)
    }

  private case class Bail(e: TypecheckError) extends Exception with NoStackTrace

  private def finalCheck(expr: ast.Expression, expectedType: Option[CT]): Expr[CT, CV] =
    check(expr).flatMap(disambiguate(_, expectedType, expr.position)) match {
      case Right(expr) => expr
      case Left(err) => throw Bail(err)
    }

  private def mostPreferredType(types: Set[CT]): CT = {
    typeInfo.typeParameterUniverse.find(types).get
  }

  private def disambiguate(exprs: Seq[Expr[CT, CV]], expectedType: Option[CT], pos: Position): Either[TypecheckError, Expr[CT, CV]] =
    expectedType match {
      case Some(t) =>
        exprs.find(_.typ == t).toRight {
          TypeMismatch(scope, canonicalName, Set(typeInfo.typeNameFor(t)), typeInfo.typeNameFor(mostPreferredType(exprs.iterator.map(_.typ).toSet)), pos)
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
              case None => Left(NoSuchColumn(scope, canonicalName, None, name, col.position))
              case Some(Environment.LookupResult(table, column, typ)) =>
                Right(Seq(Column(table, column, typ)(col.position)))
            }
        }
      case col@ast.ColumnOrAliasRef(Some(qual), name) =>
        val trueQual = ResourceName(qual.substring(TableName.PrefixIndex))
        env.lookup(trueQual, name) match {
          case None => Left(NoSuchColumn(scope, canonicalName, Some(trueQual), name, col.position))
          case Some(Environment.LookupResult(table, column, typ)) =>
            Right(Seq(Column(table, column, typ)(col.position)))
        }
      case l: ast.Literal =>
        squash(typeInfo.potentialExprs(l), l.position)
      case hole@ast.Hole.UDF(name) =>
        udfParams.get(name) match {
          case Some(expr) => Right(Seq(expr(hole.position)))
          case None => Left(UnknownUDFParameter(scope, canonicalName, name, hole.position))
        }
      case hole@ast.Hole.SavedQuery(name, view) =>
        userParameter(view.map(CanonicalName), name, hole.position)
    }
  }

  private def userParameter(view: Option[CanonicalName], name: HoleName, position: Position) = {
    val canonicalView = view.orElse(canonicalName)

    val paramSet =
      canonicalView match {
        case Some(view) =>
          userParameters.qualified.get(view)
        case None =>
          Some(userParameters.unqualified)
      }

    paramSet.flatMap(_.get(name)) match {
      case Some(UserParameters.Null(t)) => Right(Seq(NullLiteral(t)(position)))
      case Some(UserParameters.Value(v)) => Right(Seq(LiteralValue(v)(position)(typeInfo.hasType)))
      case None => Left(UnknownUserParameter(scope, canonicalName, canonicalView, name, position))
    }
  }

  private def checkFuncall(fc: ast.FunctionCall): Either[TypecheckError, Seq[Expr[CT, CV]]] = {
    val ast.FunctionCall(name, parameters, filter, window) = fc

    val typedParameters = parameters.map(check).map {
      case Left(tm) => return Left(tm)
      case Right(es) => es
    }

    val typedWindow: Option[(Seq[Expr[CT, CV]], Seq[OrderBy[CT, CV]], Seq[ast.Expression])] = window.map { w =>
      val typedPartitions = w.partitions.map(finalCheck(_, None))
      val typedOrderings = w.orderings.map(ob => OrderBy(finalCheck(ob.expression, Some(typeInfo.boolType)), ob.ascending, ob.nullLast))
      (typedPartitions, typedOrderings, w.frames)
    }

    val typedFilter: Option[Expr[CT, CV]] = filter.map(finalCheck(_, Some(typeInfo.boolType)))

    val options = functionInfo.functionsWithArity(name, typedParameters.length)

    if(options.isEmpty) {
      return Left(NoSuchFunction(scope, canonicalName, name, typedParameters.length, fc.functionNamePosition))
    }

    val (failed, resolved) = divide(funcallTypechecker.resolveOverload(options, typedParameters.map(_.map(_.typ).toSet))) {
      case Passed(f) => Right(f)
      case tm: TypeMismatchFailure[CT] => Left(tm)
    }

    if(resolved.isEmpty) {
      val TypeMismatchFailure(expected, found, idx) = failed.maxBy(_.idx)
      return Left(TypeMismatch(scope, canonicalName, expected.map(typeInfo.typeNameFor), typeInfo.typeNameFor(mostPreferredType(found)), parameters(idx).position))
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
          case (Some(boolExpr), None) =>
            if(f.needsWindow) {
              return Left(RequiresWindow(scope, canonicalName, fc.functionName, fc.functionNamePosition))
            }
            if(!f.isAggregate && !f.needsWindow) {
              return Left(NonAggregate(scope, canonicalName, fc.functionName, fc.functionNamePosition))
            }
            AggregateFunctionCall(f, params, false, Some(boolExpr))(fc.position, fc.functionNamePosition)
          case (maybeFilter, Some((partitions, orderings, frames))) =>
            if(!f.needsWindow && !f.isAggregate) {
              return Left(NonWindowFunction(scope, canonicalName, fc.functionName, fc.functionNamePosition))
            }

            // blearghhh ok so the parser does in fact parse this
            // (i.e., we know this is well-formed, assuming the input
            // came out of the parser) but it gives it to use as text
            // rather than something useful so we have to re-parse it
            // here.
            //
            // Once this is the only typechecker that can hopefully be
            // revisited.

            object RangeRowsGroups {
              def unapply(s: ast.Expression) =
                s match {
                  case ast.StringLiteral("RANGE") => Some(FrameContext.Range)
                  case ast.StringLiteral("ROWS") => Some(FrameContext.Rows)
                  case ast.StringLiteral("GROUPS") => Some(FrameContext.Groups)
                  case _ => None
                }
            }

            def frameBound(f: Seq[ast.Expression]): (FrameBound, Seq[ast.Expression]) =
              f match {
                case Seq(ast.StringLiteral("UNBOUNDED"), ast.StringLiteral("PRECEDING"), rest @ _*) =>
                  (FrameBound.UnboundedPreceding, rest)
                case Seq(ast.NumberLiteral(n), ast.StringLiteral("PRECEDING"), rest @ _*) =>
                  (FrameBound.Preceding(n.min(Long.MaxValue).max(Long.MinValue).toLong), rest)
                case Seq(ast.StringLiteral("CURRENT"), ast.StringLiteral("ROW"), rest @ _*) =>
                  (FrameBound.CurrentRow, rest)
                case Seq(ast.NumberLiteral(n), ast.StringLiteral("FOLLOWING"), rest @ _*) =>
                  (FrameBound.Following(n.min(Long.MaxValue).max(Long.MinValue).toLong), rest)
                case Seq(ast.StringLiteral("UNBOUNDED"), ast.StringLiteral("FOLLOWING"), rest @ _*) =>
                  (FrameBound.UnboundedFollowing, rest)
                case _ =>
                  malformedFrames(frames)
              }

            def and(f: Seq[ast.Expression]): Seq[ast.Expression] =
              f match {
                case Seq(ast.StringLiteral("AND"), rest @ _*) => rest
                case _ => malformedFrames(frames)
              }

            def optFrameExclusion(f: Seq[ast.Expression]): Option[FrameExclusion] =
              f match {
                case Seq(ast.StringLiteral("EXCLUDE"), ast.StringLiteral("CURRENT"), ast.StringLiteral("ROW")) => Some(FrameExclusion.CurrentRow)
                case Seq(ast.StringLiteral("EXCLUDE"), ast.StringLiteral("GROUP")) => Some(FrameExclusion.Group)
                case Seq(ast.StringLiteral("EXCLUDE"), ast.StringLiteral("TIES")) => Some(FrameExclusion.Ties)
                case Seq(ast.StringLiteral("EXCLUDE"), ast.StringLiteral("NO"), ast.StringLiteral("OTHERS")) => Some(FrameExclusion.NoOthers)
                case Seq() => None
                case _=> malformedFrames(frames)
              }

            val parsedFrames =
              frames.headOption match {
                case None =>
                  None
                case Some(RangeRowsGroups(frameContext)) =>
                  frames.tail match {
                    case Seq(ast.StringLiteral("BETWEEN"), rest @ _*) =>
                      val (start, rest1) = frameBound(rest)
                      val rest2 = and(rest1)
                      val (end, rest3) = frameBound(rest2)
                      val excl = optFrameExclusion(rest3)
                      Some(Frame(frameContext, start, Some(end), excl))
                    case rest @ Seq(_, _ @ _*) =>
                      val (start, rest1) = frameBound(rest)
                      val excl = optFrameExclusion(rest1)
                      Some(Frame(frameContext, start, None, excl))
                    case Seq() =>
                      malformedFrames(frames)
                  }
                case _ =>
                  malformedFrames(frames)
              }

            if(parsedFrames.map(_.context) == FrameContext.Groups && orderings.isEmpty) {
              return Left(GroupsRequiresOrderBy(scope, canonicalName, frames.head.position))
            }

            WindowedFunctionCall(f, params, maybeFilter, partitions, orderings, parsedFrames)(fc.position, fc.functionNamePosition)
          case (None, None) =>
            if(f.needsWindow) {
              return Left(RequiresWindow(scope, canonicalName, fc.functionName, fc.functionNamePosition))
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

  private def malformedFrames(frames: Seq[ast.Expression]): Nothing =
    throw new Exception(s"Malformed frames: {frames}")
}
