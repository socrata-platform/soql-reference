package com.socrata.soql.typechecker

import com.socrata.soql.ast._
import com.socrata.soql.exceptions._
import com.socrata.soql.typed
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}

class Typechecker[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type])(implicit ctx: String => DatasetContext[Type]) extends ((Expression, Map[ColumnName, typed.CoreExpr[ColumnName, Type]]) => typed.CoreExpr[ColumnName, Type]) { self =>
  import typeInfo._

  type Expr = typed.CoreExpr[ColumnName, Type]

  val functionCallTypechecker = new FunctionCallTypechecker(typeInfo, functionInfo)

  def apply(e: Expression, aliases: Map[ColumnName, Expr]) = typecheck(e, aliases) match {
    case Left(tm) => throw tm
    case Right(es) => disambiguate(es)
  }

  // never returns an empty value
  private def typecheck(e: Expression, aliases: Map[ColumnName, Expr]): Either[TypecheckException, Seq[Expr]] = e match {
    case r@ColumnOrAliasRef(qual, col) =>
      aliases.get(col) match {
        case Some(typed.ColumnRef(aQual, name, typ)) if name == col && qual == aQual =>
          // special case: if this is an alias that refers directly to itself, position the typed tree _here_
          // (as if there were no alias at all) to make error messages that much clearer.  This will only catch
          // semi-implicitly assigned aliases, so it's better anyway.
          Right(Seq(typed.ColumnRef(aQual, col, typ)(r.position)))
        case Some(typed.ColumnRef(aQual, name, typ)) if name == col && qual != aQual =>
          typecheck(e, Map.empty) // TODO: Revisit aliases from multiple schemas
        case Some(tree) =>
          Right(Seq(tree))
        case None =>
          val columns = ctx(qual.getOrElse(TableName.PrimaryTable.qualifier)).schema
          columns.get(col) match {
            case Some(typ) =>
              Right(Seq(typed.ColumnRef(qual, col, typ)(r.position)))
            case None =>
              throw NoSuchColumn(col, r.position)
          }
      }
    case FunctionCall(SpecialFunctions.Parens, params) =>
      assert(params.length == 1, "Parens with more than one parameter?!")
      typecheck(params(0), aliases)
    case fc@FunctionCall(name, parameters) =>
      val typedParameters = parameters.map(typecheck(_, aliases)).map {
        case Left(tm) => return Left(tm)
        case Right(es) => es
      }

      val options = functionInfo.functionsWithArity(name, typedParameters.length)
      if(options.isEmpty) return Left(NoSuchFunction(name, typedParameters.length, fc.functionNamePosition))
      val (failed, resolved) = divide(functionCallTypechecker.resolveOverload(options, typedParameters.map(_.map(_.typ).toSet))) {
        case Passed(f) => Right(f)
        case tm: TypeMismatchFailure[Type] => Left(tm)
      }
      if(resolved.isEmpty) {
        val TypeMismatchFailure(expected, found, idx) = failed.maxBy(_.idx)
        Left(TypeMismatch(name, typeNameFor(found.head), parameters(idx).position))
      } else {
        Right(resolved.flatMap { f =>
          val selectedParameters = (f.allParameters, typedParameters).zipped.map { (expected, options) =>
            val choices = options.filter(_.typ == expected)
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

          selectedParameters.foldRight(Seq(List.empty[Expr])) { (choices, remainingParams) =>
            choices.flatMap { choice => remainingParams.map(choice :: _) }
          }.map(typed.FunctionCall(f, _)(fc.position, fc.functionNamePosition))
        })
      }
    case bl@BooleanLiteral(b) =>
      Right(booleanLiteralExpr(b, bl.position))
    case sl@StringLiteral(s) =>
      Right(stringLiteralExpr(s, sl.position))
    case nl@NumberLiteral(n) =>
      Right(numberLiteralExpr(n, nl.position))
    case nl@NullLiteral() =>
      Right(nullLiteralExpr(nl.position))
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
