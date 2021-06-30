package com.socrata.soql.typechecker

import com.socrata.soql.ast._
import com.socrata.soql.exceptions._
import com.socrata.soql.{AnalysisDeserializer, typed}
import com.socrata.soql.environment.{ColumnName, DatasetContext, FunctionName, TableName}

class Typechecker[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type])(implicit ctx: Map[String, DatasetContext[Type]]) extends ((Expression, Map[ColumnName, typed.CoreExpr[ColumnName, Type]], Option[TableName]) => typed.CoreExpr[ColumnName, Type]) { self =>
  import typeInfo._

  type Expr = typed.CoreExpr[ColumnName, Type]

  val functionCallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(e: Expression, aliases: Map[ColumnName, Expr], from: Option[TableName]) = typecheck(e, aliases, from) match {
    case Left(tm) => throw tm
    case Right(es) => disambiguate(es)
  }

  // never returns an empty value
  private def typecheck(e: Expression, aliases: Map[ColumnName, Expr], from: Option[TableName]): Either[TypecheckException, Seq[Expr]] = e match {
    case r@ColumnOrAliasRef(qual, col) =>
      aliases.get(col) match {
        case Some(typed.ColumnRef(aQual, name, typ)) if name == col && qual == aQual =>
          // special case: if this is an alias that refers directly to itself, position the typed tree _here_
          // (as if there were no alias at all) to make error messages that much clearer.  This will only catch
          // semi-implicitly assigned aliases, so it's better anyway.
          Right(Seq(typed.ColumnRef(aQual, col, typ)(r.position)))
        case Some(typed.ColumnRef(aQual, name, typ)) if name == col && qual != aQual =>
          typecheck(e, Map.empty, from) // TODO: Revisit aliases from multiple schemas
        case Some(tree) =>
          Right(Seq(tree))
        case None =>
          val (theQual, implicitQual) = (qual, from) match {
            case (Some(q), _) => (q, Some(q))
            case (None, Some(TableName(_, Some(a)))) => (TableName.PrimaryTable.qualifier, Some(a))
            case (None, _) => (TableName.PrimaryTable.qualifier, None)
          }
          val columns = ctx.getOrElse(theQual, throw NoSuchTable(theQual, r.position)).schema
          columns.get(col) match {
            case Some(typ) =>
              Right(Seq(typed.ColumnRef(implicitQual, col, typ)(r.position)))
            case None =>
              throw NoSuchColumn(col, r.position)
          }
      }
    case FunctionCall(SpecialFunctions.Parens, params, window) =>
      assert(params.length == 1, "Parens with more than one parameter?!")
      assert(window.isEmpty, "Window clause exists?!")
      typecheck(params(0), aliases, from)
    case fc@FunctionCall(SpecialFunctions.Case, params, None) =>
      val actualParams =
        if(params.takeRight(2).headOption == Some(BooleanLiteral(true)(fc.position))) {
          params
        } else {
          params.dropRight(2)
        }
      typecheck(fc.copy(functionName = SpecialFunctions.CasePostTypecheck, parameters=actualParams)(position = fc.position, functionNamePosition = fc.functionNamePosition), aliases, from)
    case fc@FunctionCall(SpecialFunctions.Subscript, Seq(base, StringLiteral(prop)), window) =>
      assert(window.isEmpty, "Window clause exists?!")
      // Subscripting special case.  Some types have "subfields" that
      // can be accessed with dot notation.  The parser turns this
      // into a subscript operator with a string literal parameter.
      // Here's where we find that that corresponds to a field access.
      // If we don't find anything that works, we pretend that we
      // never did this and just typecheck it as a subscript access.
      typecheck(base, aliases, from).right.flatMap { basePossibilities =>
        val asFieldAccesses =
          basePossibilities.flatMap { basePossibility =>
            val typ = basePossibility.typ
            val fnName = SpecialFunctions.Field(typeNameFor(typ), prop)
            typecheckFuncall(fc.copy(functionName = fnName, parameters = Seq(base))(fc.position, fc.functionNamePosition), aliases, from).right.getOrElse(Nil)
          }
        val rawSubscript = typecheckFuncall(fc, aliases, from)
        if(asFieldAccesses.isEmpty) {
          rawSubscript
        } else {
          rawSubscript match {
            case Left(_) => Right(asFieldAccesses)
            case Right(asSubscripts) => Right(asFieldAccesses ++ asSubscripts)
          }
        }
      }
    case fc@FunctionCall(_, _, _) =>
      typecheckFuncall(fc, aliases, from)
    case bl@BooleanLiteral(b) =>
      Right(booleanLiteralExpr(b, bl.position))
    case sl@StringLiteral(s) =>
      Right(stringLiteralExpr(s, sl.position))
    case nl@NumberLiteral(n) =>
      Right(numberLiteralExpr(n, nl.position))
    case nl@NullLiteral() =>
      Right(nullLiteralExpr(nl.position))
    case Hole(_) =>
      throw new UnexpectedHole()
  }

  def typecheckFuncall(fc: FunctionCall, aliases: Map[ColumnName, Expr], from: Option[TableName]): Either[TypecheckException, Seq[Expr]] = {

    val FunctionCall(name, parameters, window) = fc

    val typedParameters = parameters.map(typecheck(_, aliases, from)).map {
      case Left(tm) => return Left(tm)
      case Right(es) => es
    }

    val typedWindow = window.map { w =>
      val typedPartitions = w.partitions.map(apply(_, aliases, from))
      val typedOrderings = w.orderings.map(ob => typed.OrderBy(apply(ob.expression, aliases, from), ob.ascending, ob.nullLast) )
      val typedFrames = w.frames.map { x => apply(x, aliases, from) }
      typed.WindowFunctionInfo(typedPartitions, typedOrderings, typedFrames)
    }

    val options = functionInfo.functionsWithArity(name, typedParameters.length)
    if(options.isEmpty) return Left(NoSuchFunction(name, typedParameters.length, fc.functionNamePosition))
    val (failed, resolved) = divide(functionCallTypechecker.resolveOverload(options, typedParameters.map(_.map(_.typ).toSet))) {
      case Passed(f) => Right(f)
      case tm: TypeMismatchFailure[Type] => Left(tm)
    }
    if(resolved.isEmpty) {
      val TypeMismatchFailure(expected, found, idx) = failed.maxBy(_.idx)
      Left(TypeMismatch(name, typeNameFor(typeInfo.typeParameterUniverse.find(found).get), parameters(idx).position))
    } else {
      // validate window function call
      window match {
        case Some(_) =>
          val candidateFn = resolved.head
          if (!candidateFn.needsWindow && !candidateFn.isAggregate) {
            throw FunctionDoesNotAcceptWindowInfo(name, fc.position)
          }
        case None =>
          if (functionInfo.windowFunctions(name)) {
            throw FunctionRequiresWindowInfo(name, fc.position)
          }
      }

      val potentials = resolved.flatMap { f =>
        val skipTypeCheckAfter = typedParameters.size
        val selectedParameters = (f.allParameters, typedParameters, Stream.from(0)).zipped.map { (expected, options, idx) =>
          val choices = if (idx < skipTypeCheckAfter) options.filter(_.typ == expected)
                        else options.headOption.toSeq // any type is ok for window functions
          if(choices.isEmpty) sys.error("Can't happen, we passed typechecking")
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
        }.map(typed.FunctionCall(f, _, typedWindow)(fc.position, fc.functionNamePosition))
      }

      // If all possibilities result in the same type, we can disambiguate here.
      // In principle, this could be
      //   potentials.groupBy(_.typ).values.map(disambiguate)
      // which would be strictly more general, but I am uncertain whether
      // that will preserve the preference-order assumption that disambiguate
      // relies on.
      val collapsed = if(potentials.forall(_.typ == potentials.head.typ)) Seq(disambiguate(potentials)) else potentials

      Right(collapsed)
    }
  }

  def divide[T, L, R](xs: TraversableOnce[T])(f: T => Either[L, R]): (Seq[L], Seq[R]) = {
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

  def disambiguate(choices: Seq[Expr]): Expr = {
    // technically we should use typeInfo.typeParameterUniverse to determine which
    // we prefer, but as it happens these are constructed such that more preferrred
    // things happen to come first anyway.
    val minSize = choices.minBy(_.size).size
    val minimal = choices.filter(_.size == minSize)
    minimal.lengthCompare(1) match {
      case 1 => minimal.head
      case n => minimal.head
    }
  }
}
