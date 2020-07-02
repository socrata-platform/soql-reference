package com.socrata.soql.typechecker

import com.socrata.soql.functions._
import com.socrata.soql.typed.Typable

sealed abstract class CandidateEvaluation[Type]
case class TypeMismatchFailure[Type](expected: Set[Type], found: Set[Type], idx: Int) extends CandidateEvaluation[Type] {
  require(found.intersect(expected).isEmpty, "type mismatch but found in expected")
}
case class Passed[Type](function: MonomorphicFunction[Type]) extends CandidateEvaluation[Type]

class FunctionCallTypechecker[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[FunctionCallTypechecker[_]])

  type Func = Function[Type]
  type MFunc = MonomorphicFunction[Type]
  type Val = Typable[Type]

  import typeInfo._
  import functionInfo._

  def goodArity(function: Func, parameters: Seq[_]): Boolean = {
    if(function.isVariadic) {
      function.minArity <= parameters.length
    } else {
      function.minArity == parameters.length
    }
  }

  /** Resolve a set of overloads into a single candidate.  If you have only one, you should use
    * `evaluateCandidate` directly, as this loses type mismatch information. */
  def resolveOverload(candidates: Set[Func], parameters: Seq[Set[Type]]): Seq[CandidateEvaluation[Type]] = {
    require(candidates.nonEmpty, "empty candidate set")
    require(candidates.forall { candidate => candidate.name == candidates.iterator.next().name }, "differently-named functions") // this is _overload_ resolution!
    require(candidates.forall(goodArity(_, parameters)), "bad candidate arity")

    // "good" will be a list of pairs of functions together with the
    // conversions necessary to call those functions with these
    // parameters.
    candidates.toSeq.flatMap(evaluateCandidate(_, parameters))
  }

  def evaluateCandidate(candidate: Func, parameters: Seq[Set[Type]]): Seq[CandidateEvaluation[Type]] = {
    require(goodArity(candidate, parameters))

    // yay combinatorial explosion!  Fortunately in practice we'll only ever have one or _maybe_ two type parameters.
    def loop(remaining: List[String], bindings: Map[String, Type]): Iterator[CandidateEvaluation[Type]] = {
      remaining match {
        case hd :: tl =>
          typeParameterUniverse.iterator.filter(candidate.constraints.getOrElse(hd, typeParameterUniverse)).flatMap { typ =>
            loop(tl, bindings + (hd -> typ))
          }
        case Nil =>
          Iterator(evaluateMonomorphicCandidate(MonomorphicFunction(candidate, bindings), parameters))
      }
    }
    loop(candidate.typeParameters.toList, Map.empty).toSeq
  }

  def evaluateMonomorphicCandidate(candidate: MFunc, parameters: Seq[Set[Type]]): CandidateEvaluation[Type] = {
    require(goodArity(candidate.function, parameters))

    val checkedParameters = parameters
    val checkedCandidateAllParameters = candidate.allParameters

    (checkedCandidateAllParameters, checkedParameters, Stream.from(0)).zipped.foreach { (expectedType, availableTypes, idx) =>
      if(!availableTypes(expectedType)) {
        return TypeMismatchFailure(Set(expectedType), availableTypes, idx)
      }
    }
    log.debug("Successfully type-checked {}", candidate.name)
    Passed(candidate)
  }
}
