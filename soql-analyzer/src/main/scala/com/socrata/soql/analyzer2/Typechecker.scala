package com.socrata.soql.analyzer2

import scala.util.control.NoStackTrace
import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.ast
import com.socrata.soql.collection.{OrderedMap, CovariantSet}
import com.socrata.soql.environment.{ColumnName, HoleName, ResourceName, Source, TableName, FunctionName, Provenance}
import com.socrata.soql.functions.FunctionType
import com.socrata.soql.typechecker.{TypeInfo2, FunctionInfo, FunctionCallTypechecker, Passed, TypeMismatchFailure}

class Typechecker[MT <: MetaTypes](
  sourceName: Option[types.ScopedResourceName[MT]],
  canonicalName: Option[CanonicalName],
  primaryTable: Option[Provenance],
  env: Environment[MT],
  namedExprs: Map[ColumnName, Expr[MT]],
  udfParams: Map[HoleName, (Option[types.ScopedResourceName[MT]], Position) => Expr[MT]],
  userParameters: UserParameters[MT#ColumnType, MT#ColumnValue],
  typeInfo: TypeInfo2[MT],
  functionInfo: FunctionInfo[MT#ColumnType]
) extends ExpressionUniverse[MT] {
  type Error = TypecheckError[RNS]
  private val TypecheckError = SoQLAnalyzerError.TypecheckError
  import TypecheckError._

  private val funcallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(expr: ast.Expression, expectedType: Option[CT]): Either[Error, Expr] =
    try {
      Right(finalCheck(expr, expectedType))
    } catch {
      case Bail(e) =>
        Left(e)
    }

  private case class Bail(e: Error) extends Exception with NoStackTrace

  private def finalCheck(expr: ast.Expression, expectedType: Option[CT]): Expr =
    check(expr).flatMap(disambiguate(_, expectedType, expr.position)) match {
      case Right(expr) => expr
      case Left(err) => throw Bail(err)
    }

  private def mostPreferredType(types: Set[CT]): CT = {
    typeInfo.typeParameterUniverse.find(types).get
  }

  private def disambiguate(exprs: Seq[Expr], expectedType: Option[CT], pos: Position): Either[Error, Expr] = {
    val choices =
      expectedType match {
        case Some(t) =>
          val candidates = exprs.filter(_.typ == t)
          if(candidates.isEmpty) {
            return Left(
              TypeMismatch(Source.nonSynthetic(sourceName, pos), Set(typeInfo.typeNameFor(t)), typeInfo.typeNameFor(mostPreferredType(exprs.iterator.map(_.typ).toSet)))
            )
          }
          candidates
        case None =>
          exprs
      }

    val minSize = choices.minBy(_.size).size
    val minimal = choices.filter(_.size == minSize)
    minimal.lengthCompare(1) match {
      case 1 =>
        Right(minimal.head)
      case n =>
        Right(minimal.minBy { e => typeInfo.typeParameterUniverseIndices(e.typ) })
    }
  }

  private def squash(exprs: Seq[Expr], pos: Position): Either[Error, Seq[Expr]] = {
    var acc = OrderedMap.empty[CT, Expr]

    for { e <- exprs } {
      acc.get(e.typ) match {
        case Some(e2) if e2.size <= e.size =>
          // the smaller (or at least most-preferred) is already chosen
        case Some(_) | None =>
          acc += e.typ -> e
      }
    }

    Right(acc.valuesIterator.toVector)
  }

  private def check(expr: ast.Expression): Either[Error, Seq[Expr]] = {
    expr match {
      case ast.FunctionCall(ast.SpecialFunctions.Parens, Seq(param), None, None) =>
        check(param)
      case fc@ast.FunctionCall(ast.SpecialFunctions.Case, params, _, None) =>
        // The parser will always produce a case form that ends with
        // either (true, elseExpr) or (false, null) - it does this so
        // that the AST can be pretty-printed back into the same shape
        // as the user provided; specifically if what the user
        // provided ended with `when true then whatever` then it
        // _won't_ get pretty-printed back as `else whatever` because
        // that won't be the last clause.  But now that we're
        // typechecking, we can discard that last (false, null) if it
        // exists.
        val actualParams =
          if(params.takeRight(2).headOption == Some(ast.BooleanLiteral(true)(fc.position))) {
            params
          } else {
            params.dropRight(2)
          }
        check(fc.copy(functionName = ast.SpecialFunctions.CasePostTypecheck, parameters=actualParams)(position = fc.position, functionNamePosition = fc.functionNamePosition))
      case fc@ast.FunctionCall(ast.SpecialFunctions.Subscript, Seq(base, ast.StringLiteral(prop)), None, None) =>
        // experimental, currently disabled: allow expressions of the
        // form `a.b` to reference column b on table a.  Get rid of
        // the "if false" in the match to enable this.
        val asColumnRef =
          base match {
            case ast.ColumnOrAliasRef(None, name) if false =>
              // ok, this _might_ be a table column reference actually!
              check(ast.ColumnOrAliasRef(Some("_" + name), ColumnName(prop))(fc.position)) match {
                case Right(checked) => Some(checked)
                case Left(_) => None
              }
            case _ =>
              None
          }

        val asExpr =
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

        (asColumnRef, asExpr) match {
          case (Some(_), Right(_)) => throw new Exception("oops") // TODO: ambiguous
          case (Some(checked), Left(_)) => Right(checked)
          case (None, other) => other
        }
      case fc: ast.FunctionCall =>
        checkFuncall(fc).flatMap(squash(_, fc.position))
      case col@ast.ColumnOrAliasRef(None, name) =>
        namedExprs.get(name) match {
          case Some(prechecked) =>
            Right(Seq(prechecked.reReference(Source.nonSynthetic(sourceName, col.position))))
          case None =>
            env.lookup(name) match {
              case None => Left(NoSuchColumn(Source.nonSynthetic(sourceName, col.position), None, name, Util.possibilitiesFor(env, namedExprs.keysIterator, None, name)))
              case Some(Environment.LookupResult.Virtual(table, column, typ)) =>
                Right(Seq(VirtualColumn(table, column, typ)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, col.position)))))
              case Some(Environment.LookupResult.Physical(tableName, tableLabel, column, typ)) =>
                Right(Seq(PhysicalColumn[MT](tableLabel, tableName, column, typ)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, col.position)))))
            }
        }
      case col@ast.ColumnOrAliasRef(Some(qual), name) =>
        val trueQual = ResourceName(qual.substring(TableName.PrefixIndex))
        env.lookup(trueQual, name) match {
          case None => Left(NoSuchColumn(Source.nonSynthetic(sourceName, col.position), Some(trueQual), name, Util.possibilitiesFor(env, namedExprs.keysIterator, Some(trueQual), name)))
          case Some(Environment.LookupResult.Virtual(table, column, typ)) =>
            Right(Seq(VirtualColumn(table, column, typ)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, col.position)))))
          case Some(Environment.LookupResult.Physical(tableName, tableLabel, column, typ)) =>
            Right(Seq(PhysicalColumn[MT](tableLabel, tableName, column, typ)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, col.position)))))
        }
      case l: ast.Literal =>
        squash(typeInfo.potentialExprs(l, sourceName, primaryTable), l.position)
      case hole@ast.Hole.UDF(name) =>
        udfParams.get(name) match {
          case Some(expr) => Right(Seq(expr(sourceName, hole.position)))
          case None => Left(UnknownUDFParameter(Source.nonSynthetic(sourceName, hole.position), name))
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
      case Some(UserParameters.Null(t)) => Right(Seq(NullLiteral(t)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, position)))))
      case Some(UserParameters.Value(v)) => Right(Seq(LiteralValue(v)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, position)))(typeInfo.hasType)))
      case None => Left(UnknownUserParameter(Source.nonSynthetic(sourceName, position), canonicalView, name))
    }
  }

  private def checkFuncall(fc: ast.FunctionCall): Either[Error, Seq[Expr]] = {
    val ast.FunctionCall(name, parameters, filter, window) = fc

    val typedParameters = parameters.map(check).map {
      case Left(tm) => return Left(tm)
      case Right(es) => es
    }

    val typedWindow: Option[(Seq[Expr], Seq[OrderBy], Seq[ast.Expression])] = window.map { w =>
      val typedPartitions = w.partitions.map(finalCheck(_, None))
      val typedOrderings = w.orderings.map { ob =>
        val typedExpr = finalCheck(ob.expression, None)
        if(!typeInfo.isOrdered(typedExpr.typ)) {
          return Left(UnorderedOrderBy(Source.nonSynthetic(sourceName, ob.expression.position), typeInfo.typeNameFor(typedExpr.typ)))
        }
        OrderBy(typedExpr, ob.ascending, ob.nullLast)
      }
      (typedPartitions, typedOrderings, w.frames)
    }

    val typedFilter: Option[Expr] = filter.map(finalCheck(_, Some(typeInfo.boolType)))

    val options = functionInfo.functionsWithArity(name, typedParameters.length)

    if(options.isEmpty) {
      return Left(NoSuchFunction(Source.nonSynthetic(sourceName, fc.functionNamePosition), name, typedParameters.length))
    }

    val (failed, resolved) = divide(funcallTypechecker.resolveOverload(options, typedParameters.map(_.map(_.typ).toSet))) {
      case Passed(f) => Right(f)
      case tm: TypeMismatchFailure[CT] => Left(tm)
    }

    if(resolved.isEmpty) {
      val TypeMismatchFailure(expected, found, idx) = failed.maxBy(_.idx)
      return Left(TypeMismatch(Source.nonSynthetic(sourceName, parameters(idx).position), expected.map(typeInfo.typeNameFor), typeInfo.typeNameFor(mostPreferredType(found))))
    }

    val potentials = resolved.flatMap { f =>
      val skipTypeCheckAfter = typedParameters.size
      val selectedParameters: Iterator[Seq[Expr]] = Iterator.from(0).zip(f.allParameters.iterator).zip(typedParameters.iterator).map { case ((idx, expected), options) =>
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

      selectedParameters.toVector.foldRight(Seq(List.empty[Expr])) { (choices, remainingParams) =>
        choices.flatMap { choice => remainingParams.map(choice :: _) }
      }.map { params: Seq[Expr] =>
        (typedFilter, typedWindow) match {
          case (Some(boolExpr), None) =>
            f.functionType match {
              case FunctionType.Window(_) =>
                return Left(RequiresWindow(Source.nonSynthetic(sourceName, fc.functionNamePosition), fc.functionName))
              case FunctionType.Normal =>
                return Left(NonAggregateFunction(Source.nonSynthetic(sourceName, fc.functionNamePosition), fc.functionName))
              case FunctionType.Aggregate =>
                AggregateFunctionCall(f, params, false, Some(boolExpr))(new FuncallPositionInfo(Source.nonSynthetic(sourceName, fc.position), fc.functionNamePosition))
            }
          case (maybeFilter, Some((partitions, orderings, frames))) =>
            val frameAllowed =
              f.functionType match {
                case FunctionType.Normal =>
                  return Left(NonWindowFunction(Source.nonSynthetic(sourceName, fc.functionNamePosition), fc.functionName))
                case FunctionType.Aggregate =>
                  true
                case FunctionType.Window(frameAllowed) =>
                  frameAllowed
              }

            if(fc.distinct) {
              return Left(DistinctWithOver(Source.nonSynthetic(sourceName, fc.functionNamePosition)))
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

                      if(start == FrameBound.UnboundedFollowing) {
                        return Left(IllegalStartFrameBound(Source.nonSynthetic(sourceName, frames.head.position), start.text))
                      }
                      if(end == FrameBound.UnboundedPreceding) {
                        return Left(IllegalEndFrameBound(Source.nonSynthetic(sourceName, rest2.head.position), end.text))
                      }
                      if(start.level > end.level) {
                        return Left(MismatchedFrameBound(Source.nonSynthetic(sourceName, rest2.head.position), start.text, end.text))
                      }

                      val excl = optFrameExclusion(rest3)
                      Some(Frame(frameContext, start, Some(end), excl))
                    case rest @ Seq(_, _ @ _*) =>
                      val (start, rest1) = frameBound(rest)

                      if(start == FrameBound.UnboundedFollowing) {
                        return Left(IllegalStartFrameBound(Source.nonSynthetic(sourceName, frames.head.position), start.text))
                      }

                      val excl = optFrameExclusion(rest1)
                      Some(Frame(frameContext, start, None, excl))
                    case Seq() =>
                      malformedFrames(frames)
                  }
                case _ =>
                  malformedFrames(frames)
              }

            if(parsedFrames.map(_.context) == FrameContext.Groups && orderings.isEmpty) {
              return Left(GroupsRequiresOrderBy(Source.nonSynthetic(sourceName, frames.head.position)))
            }

            if(parsedFrames.isDefined && !frameAllowed) {
              // TODO: reject once we've verified that this isn't being used by anything
            }

            WindowedFunctionCall(f, params, maybeFilter, partitions, orderings, parsedFrames)(new FuncallPositionInfo(Source.nonSynthetic(sourceName, fc.position), fc.functionNamePosition))
          case (None, None) =>
            f.functionType match {
              case FunctionType.Window(_) =>
                return Left(RequiresWindow(Source.nonSynthetic(sourceName, fc.functionNamePosition), fc.functionName))
              case FunctionType.Aggregate =>
                AggregateFunctionCall(f, params, false, None)(new FuncallPositionInfo(Source.nonSynthetic(sourceName, fc.position), fc.functionNamePosition))
              case FunctionType.Normal =>
                FunctionCall(f, params)(new FuncallPositionInfo(Source.nonSynthetic(sourceName, fc.position), fc.functionNamePosition))
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
