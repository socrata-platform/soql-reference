package com.socrata.soql.typechecker

import com.socrata.soql.functions._
import com.socrata.soql.typed.Typable

object UtilTypes {
  type ConversionSet[Type] = Seq[Option[MonomorphicFunction[Type]]]
}

import UtilTypes._

sealed abstract class OverloadResult[+Type]
case object NoMatch extends OverloadResult[Nothing]
case class Ambiguous[Type](candidates: Map[MonomorphicFunction[Type], ConversionSet[Type]]) extends OverloadResult[Type] {
  val conversionsRequired = candidates.head._2.size
  require(candidates.forall(_._2.size == conversionsRequired), "ambiguous functions do not all have the same number of possible conversions")
}
case class Matched[Type](function: MonomorphicFunction[Type], conversions: ConversionSet[Type]) extends OverloadResult[Type]

sealed abstract class CandidateEvaluation[Type]
case class TypeMismatchFailure[Type](expected: Set[Type], found: Type, idx: Int) extends CandidateEvaluation[Type] {
  require(!expected.contains(found), "type mismatch but found in expected")
}
case class UnificationFailure[Type](found: Type, idx: Int) extends CandidateEvaluation[Type]
case class Passed[Type](conversionSet: ConversionSet[Type]) extends CandidateEvaluation[Type]

class FunctionCallTypechecker[Type](typeInfo: FunctionTypeInfo[Type]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[FunctionCallTypechecker[_]])

  type Func = Function[Type]
  type MFunc = MonomorphicFunction[Type]
  type Val = Typable[Type]
  type ConvSet = ConversionSet[Type]

  import typeInfo._

  /** Resolve a set of overloads into a single candidate.  If you have only one, you should use
    * `evaluateCandidate` directly, as this loses type mismatch information. */
  def resolveOverload(candidates: Set[Func], parameters: Seq[Val]): OverloadResult[Type] = {
    require(candidates.nonEmpty, "empty candidate set")
    require(candidates.forall { candidate => candidate.name == candidates.iterator.next().name }, "differently-named functions") // this is _overload_ resolution!
    require(candidates.forall(_.arity == parameters.length), "bad candidate arity")

    // "good" will be a list of pairs of functions together with the
    // conversions necessary to call those functions with these
    // parameters.
    val good = for {
      candidate <- candidates.toSeq
      (mfunc, Passed(conversions)) <- evaluateCandidate(candidate, parameters)
    } yield (mfunc, conversions)

    if(good.isEmpty) {
      NoMatch
    } else {
      // grouped will be a "good" chunked by number-of-conversions-required
      val grouped = good.groupBy(_._2.count(_.isDefined))
      val minConversionsRequired = grouped.keys.min
      val bestGroup = grouped(minConversionsRequired)
      if(bestGroup.size > 1) Ambiguous[Type](bestGroup.toMap)
      else {
        val (bestFunc, conversions) = bestGroup.head
        Matched(bestFunc, conversions)
      }
    }
  }

  def evaluateCandidate(candidate: Func, parameters: Seq[Val]): Iterator[(MFunc, CandidateEvaluation[Type])] = {
    require(candidate.arity == parameters.length)

    // yay combinatorial explosion!  Fortunately in practice we'll only ever have one or _maybe_ two type parameters.
    def loop(remaining: List[String], bindings: Map[String, Type]): Iterator[(MFunc, CandidateEvaluation[Type])] = {
      remaining match {
        case hd :: tl =>
          typeParameterUniverse.filter(candidate.constraints.getOrElse(hd, typeParameterUniverse)).iterator.flatMap { typ =>
            loop(tl, bindings + (hd -> typ))
          }
        case Nil =>
          val monomorphic = MonomorphicFunction(candidate, bindings)
          Iterator(monomorphic -> evaluateMonomorphicCandidate(MonomorphicFunction(candidate, bindings), parameters))
      }
    }
    loop(candidate.typeParameters.toList, Map.empty)
  }

  def evaluateMonomorphicCandidate(candidate: MFunc, parameters: Seq[Val]): CandidateEvaluation[Type] = {
    require(candidate.arity == parameters.length)
    val parameterConversions: ConvSet = (candidate.parameters, parameters, Stream.from(0)).zipped.map { (expectedTyp, value, idx) =>
      if(canBePassedToWithoutConversion(value.typ, expectedTyp)) {
        None
      } else {
        val maybeConv = implicitConversions(value.typ, expectedTyp)
        maybeConv match {
          case Some(f) =>
            log.debug("Conversion found: {}", f.name)
            assert(f.result == expectedTyp, "conversion result is not what's expected")
            assert(f.arity == 1, "conversion is not an arity-1 function")
            assert(canBePassedToWithoutConversion(value.typ, f.parameters(0)), "conversion does not take the type to be converted")
          case None =>
            log.debug("Type mismatch, and there is no usable implicit conversion from {} to {}", value.typ:Any, expectedTyp:Any)
            return TypeMismatchFailure(Set(expectedTyp), value.typ, idx)
        }
        maybeConv
      }
    }.toSeq

    log.debug("Successfully type-checked {}; implicit conversions {}", candidate.name:Any, parameterConversions:Any)
    Passed(parameterConversions)
  }

  def narrowDownFailure(fs: Set[Func], params: Seq[Val]): UnificationFailure[Type] = {
    // the first value we return will be the first index whose
    // addition causes a type error.
    for(paramList <- params.inits.toIndexedSeq.reverse.drop(1)) {
      val n = paramList.length
      val res = resolveOverload(fs.map { f => f.copy(parameters = f.parameters.take(n)) }, paramList)
      res match {
        case NoMatch =>
          val paramIdx = n - 1
          return UnificationFailure(params(paramIdx).typ, paramIdx)
        case _ => // ok, try again
      }
    }
    sys.error("Can't get here")
  }

  /** Heuristics to try to make "null" not cause type-checking failures.
   * This is kinda evil (and since no one uses literal nulls in ambiguous
   * places in expressions ANYWAY hopefully nearly useless), but meh.  Has
   * to be done. */
  def disambiguateNulls(failure: Map[MFunc, ConvSet], parameters: Seq[Val], nullType: Type): Option[(String, MFunc, ConvSet)] = {
    case class SimpleValue(typ: Type) extends Typable[Type]

    // ok.  First, if half or more of all parameters are the same type
    // after conversion, and all the rest are nulls, try to just use
    // those values in the nulls' places (WITH implicit conversions)
    // and see if that resolves things.

    // TODO: make this "the same type post-implicit conversions" which
    // means filtering the failures down to just those instances in
    // which that is true.
    val paramsByType = parameters.groupBy(_.typ)
    if(paramsByType.size == 2 && paramsByType.contains(nullType)) {
      val typedParams = (paramsByType - nullType).head._2
      if(typedParams.size >= parameters.size/2) {
        resolveOverload(failure.keySet.filter(allParamsTheSameType(_).isDefined).map(_.function), parameters.map { p => if(p.typ == nullType) typedParams(0) else p }) match {
          case Matched(f, c) => return Some(("half or more not-null same type", f, c))
          case _ => // nothing
        }
      }
    }

    // if there's exactly one null parameter, try the types in
    // typeParameterUniverse (WITHOUT implicit conversions) until we
    // find one that succeeds.
    if(paramsByType.contains(nullType) && paramsByType(nullType).size == 1) {
      val typeInfoWithoutImplicitConversions = new FunctionTypeInfo[Type] {
        def typeParameterUniverse = typeInfo.typeParameterUniverse
        def implicitConversions(from: Type, to: Type) = None
        def canBePassedToWithoutConversion(actual: Type, expected: Type): Boolean = typeInfo.canBePassedToWithoutConversion(actual, expected)
      }
      val withoutImplicitConversions = new FunctionCallTypechecker[Type](typeInfoWithoutImplicitConversions)
      for(t <- typeParameterUniverse) {
        withoutImplicitConversions.resolveOverload(failure.map(_._1.function).toSet, parameters.map { p => if(p.typ == nullType) SimpleValue(t) else p }) match {
          case Matched(f,c) => return Some(("one null parameter", f, c))
          case _ => // nothing
        }
      }
    } else {
      return None // there was a not-null parameter, we can't do anything.
    }

    sys.error("nyi")
  }

  def allParamsTheSameType(f: MFunc): Option[Type] = {
    if(f.parameters.nonEmpty && f.parameters.forall(_ == f.parameters(0))) Some(f.parameters(0))
    else None
  }
}
