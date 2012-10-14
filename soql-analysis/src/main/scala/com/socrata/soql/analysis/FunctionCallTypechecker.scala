package com.socrata.soql.analysis

object UtilTypes {
  type ConversionSet[Type] = Seq[Option[Function[Type]]]
}

import UtilTypes._

sealed abstract class OverloadResult[+Type]
case object NoMatch extends OverloadResult[Nothing]
case class Ambiguous[Type](candidates: Seq[(Function[Type], ConversionSet[Type])]) extends OverloadResult[Type] {
  // this is really a sort of multimap -- a single function can appear
  // more than once.
  val conversionsRequired = candidates.head._2.size
  require(candidates.forall(_._2.size == conversionsRequired), "ambiguous functions do not all have the same number of possible conversions")
}
case class Matched[Type](function: Function[Type], conversions: ConversionSet[Type]) extends OverloadResult[Type]

sealed abstract class CandidateEvaluation[Type]
case class TypeMismatch[Type](expected: Set[Type], found: Type, idx: Int) extends CandidateEvaluation[Type] {
  require(!expected.contains(found), "type mismatch but found in expected")
}
case class UnificationFailure[Type](existing: Set[Type], found: Type, idx: Int) extends CandidateEvaluation[Type] {
  require(!existing.contains(found), "unification failure but found in existing")
}
case class Passed[Type](conversionSet: Seq[ConversionSet[Type]]) extends CandidateEvaluation[Type]

abstract class FunctionCallTypechecker[Type] { self =>
  val log = org.slf4j.LoggerFactory.getLogger(classOf[FunctionCallTypechecker[_]])

  type Func = Function[Type]
  type Val = Typable[Type]
  type ConvSet = ConversionSet[Type]

  def typeParameterUniverse: Set[Type] // The set of all types a function can accept (i.e., everything excluding null)
  def implicitConversions(from: Type, to: Type): Option[Func]
  def canBePassedToWithoutConversion(actual: Type, expected: Type): Boolean // this accepts at least a->a and null->a

  def resolveOverload(candidates: Set[Func], parameters: Seq[Val]): OverloadResult[Type] = {
    require(candidates.nonEmpty, "empty candidate set")
    require(candidates.forall { candidate => candidate.name == candidates.iterator.next().name }, "differently-named functions") // this is _overload_ resolution!
    require(candidates.forall(_.arity == parameters.length), "bad candidate arity")

    // "good" will be a list of pairs of functions together with the
    // conversions necessary to call those functions with these
    // parameters.
    val good = candidates.foldLeft(Vector.empty[(Func, ConvSet)]) { (results, candidate) =>
      evaluateCandidate(candidate, parameters) match {
        case Passed(conversions) =>
          results ++ conversions.map(candidate -> _)
        case _ =>
          results
      }
    }

    if(good.isEmpty) {
      NoMatch
    } else {
      // grouped will be a "good" chunked by number-of-conversions-required
      val grouped = good.groupBy(_._2.count(_.isDefined))
      val minConversionsRequired = grouped.keys.min
      val bestGroup = grouped(minConversionsRequired)
      if(bestGroup.size > 1) Ambiguous(bestGroup.distinct)
      else {
        val (bestFunc, conversions) = bestGroup.head
        Matched(bestFunc, conversions)
      }
    }
  }

  def evaluateCandidate(candidate: Func, parameters: Seq[Val]): CandidateEvaluation[Type] = {
    require(candidate.arity == parameters.length)

    // ok, first we resolve our type-parameters.  Fortunately, this is
    // equivalent to the problem of resolving overloads in a
    // type-parameter-less world!  And hey, we HAVE A CLASS TO DO
    // THAT!  It's THIS class!
    val boundTypeParameters: Map[String, Seq[Map[Int, Option[Func]]]] = candidate.parameters.collect { case VariableType(name) =>
      log.debug("Resolving type parameter {} for function {}", name: Any, candidate.name: Any)

      // Ok, if we have n actual parameters which are being passed
      // into parameters of this variable type, we'll make one n-ary
      // function for each type allowed by this variable and pass into
      // it these arguments.
      val (_, fakeParameters, parameterIndices) = (candidate.parameters, parameters, Stream.from(0)).zipped.filter { (formal, _, _) => formal == VariableType(name) }
      val fakeFunctions = candidate.variableConstraints.getOrElse(name, typeParameterUniverse).map { typ =>
        Function(candidate.name, Map.empty, Vector.fill(fakeParameters.length)(FixedType(typ)), FixedType(typ))
      }
      resolveOverload(fakeFunctions, fakeParameters) match {
        case NoMatch =>
          log.debug("Unable to resolve type parameter {}", name)
          return narrowDownFailure(candidate, fakeFunctions, fakeParameters, parameterIndices)
        case Ambiguous(fs) =>
          log.debug("Unable to resolve type parameter {} to a single type", name)
          // ok, so what I've got is a set of functions, all of which
          // require exactly the same number of implicit conversions
          // (possibly 0!  Thanks, literal null)
          val indicesAndConversions = for {
            (f, conversionSet) <- fs
          } yield parameterIndices.zip(conversionSet).toMap
          name -> indicesAndConversions
        case Matched(_, conversionSet) =>
          name -> Seq(parameterIndices.zip(conversionSet).toMap)
      }
    }.toMap

    val parameterConversions = for(permutation <- permute(boundTypeParameters)) yield {
      (candidate.parameters, parameters, Stream.from(0)).zipped.map { (formal, actual, idx) =>
        formal match {
          case FixedType(formalTyp) =>
            if(canBePassedToWithoutConversion(actual.typ, formalTyp)) {
              None
            } else {
              val maybeConv = implicitConversions(actual.typ, formalTyp)
              maybeConv match {
                case Some(f) =>
                  log.debug("Conversion found: {}", f.name)
                  assert(f.result == FixedType(formalTyp), "conversion result is not what's expected")
                  assert(f.arity == 1, "conversion is not an arity-1 function")
                  assert(f.parameters(0).isInstanceOf[FixedType[_]], "conversion does not take a fixed type")
                  assert(canBePassedToWithoutConversion(actual.typ, f.parameters(0).asInstanceOf[FixedType[Type]].typ), "conversion does not take the type to be converted")
                case None =>
                  log.debug("Type mismatch, and there is no usable implicit conversion from {} to {}", actual.typ:Any, formalTyp:Any)
                  return TypeMismatch(Set(formalTyp), actual.typ, idx)
              }
              maybeConv
            }
          case VariableType(name) =>
            permutation(name)(idx)
        }
      }
    }

    log.debug("Successfully type-checked {}; implicit conversions {}", candidate.name:Any, parameterConversions:Any)
    Passed(parameterConversions)
  }

  // A little icky; takes stack space proportional to the number of
  // elements in the map, but these maps will be small (since it's
  // proportional to the number of type parameters in the candidate's
  // argument list, which will certainly be very small for real soql
  // functions).
  def permute[K, V](m: Map[K, Seq[V]]): Seq[Map[K, V]] = {
    if(m.isEmpty) Vector(Map.empty)
    else {
      val (k, vs) = m.head
      val subPermutations = permute(m - k)
      vs.flatMap { v =>
        subPermutations.map(_ + (k -> v))
      }
    }
  }

  def narrowDownFailure(candidate: Func, fs: Set[Func], params: Seq[Val], parameterIndices: Seq[Int]): UnificationFailure[Type] = {
    // the first value we return will be the first index whose
    // addition causes a type error.
    for(paramList <- params.inits.toIndexedSeq.reverse.drop(1)) {
      val n = paramList.length
      val res = resolveOverload(fs.map { f => f.copy(parameters = f.parameters.take(n)) }, paramList)
      res match {
        case NoMatch =>
          val paramIdx = parameterIndices(n - 1)
          val name = candidate.parameters(paramIdx).asInstanceOf[VariableType].name
          val typeSet = candidate.variableConstraints.getOrElse(name, typeParameterUniverse)
          return UnificationFailure(params.take(n - 1).map(_.typ).toSet, params(n - 1).typ, paramIdx)
        case _ => // ok, try again
      }
    }
    sys.error("Can't get here")
  }

  /** Heuristics to try to make "null" not cause type-checking failures.
   * This is kinda evil (and since no one uses literal nulls in ambiguous
   * places in expressions ANYWAY hopefully nearly useless), but meh.  Has
   * to be done.
   *
   * TODO: also return the type(s) that this decided the nulls were (important
   * if it's the argument to a parameter with variable type). */
  def disambiguateNulls(failure: Seq[(Func, ConvSet)], parameters: Seq[Val], nullType: Type): Option[(String, Func, ConvSet)] = {
    case class SimpleValue(typ: Type) extends Typable[Type]

    // ok.  First, if half or more of all parameters are the same type
    // and all the rest are nulls, try to just use those values in the
    // nulls' places (WITH implicit conversions) and see if that
    // resolves things.

    // TODO: make this "the same type post-implicit conversions" which
    // means filtering the failures down to just those instances in
    // which that is true.
    val paramsByType = parameters.groupBy(_.typ)
    if(paramsByType.size == 2 && paramsByType.contains(nullType)) {
      val typedParams = (paramsByType - nullType).head._2
      if(typedParams.size >= parameters.size/2) {
        resolveOverload(failure.map(_._1).toSet, parameters.map { p => if(p.typ == nullType) typedParams(0) else p }) match {
          case Matched(f, c) => return Some(("half or more not-null same type", f, c))
          case _ => // nothing
        }
      }
    }

    // if there's exactly one null parameter, try the types in
    // typeParameterUniverse (WITHOUT implicit conversions) until we
    // find one that succeeds.
    if(paramsByType.contains(nullType) && paramsByType(nullType).size == 1) {
      val withoutImplicitConversions = new FunctionCallTypechecker[Type] {
        def typeParameterUniverse = self.typeParameterUniverse
        def implicitConversions(from: Type, to: Type) = None
        def canBePassedToWithoutConversion(actual: Type, expected: Type): Boolean = self.canBePassedToWithoutConversion(actual, expected)
      }
      for(t <- typeParameterUniverse) { // FIXME: Have some kind of preference!
        withoutImplicitConversions.resolveOverload(failure.map(_._1).toSet, parameters.map { p => if(p.typ == nullType) SimpleValue(t) else p }) match {
          case Matched(f,c) => return Some(("one null parameter", f, c))
          case _ => // nothing
        }
      }
    } else {
      return None // there was a not-null parameter, we can't do anything.
    }

    sys.error("nyi")
  }
}

object TestItAll {
  object Types extends Enumeration {
    val Number, Money, Double = Value
    val Text = Value
    val FixedTimestamp = Value
    val FloatingTimestamp = Value

    val Null = Value
    // fake literal types; these are used to tag literals since they
    // can be used more permissively than real types.  In particular,
    // using them can induce implicit conversions that would not occur
    // otherwise.  A FooLiteral can always be passed to a function
    // that expects a Foo without any conversion.
    val NumberLiteral = Value
    val numberLiterals = Set(NumberLiteral)
    val TextLiteral, TextFixedTSLiteral, TextFloatingTSLiteral = Value
    val textLiterals = Set(TextLiteral, TextFixedTSLiteral, TextFloatingTSLiteral)

    val realTypes = Set(
      Number, Money, Double,
      Text,
      FixedTimestamp,
      FloatingTimestamp
    )
  }

  import Types._

  import com.socrata.soql.FunctionName

  val TimesNumNum = Function(FunctionName("*"), Map.empty, Seq(Number, Number).map(FixedType(_)), FixedType(Number))
  val TimesNumMoney = Function(FunctionName("*"), Map.empty, Seq(Number, Money).map(FixedType(_)), FixedType(Money))
  val TimesMoneyNum = Function(FunctionName("*"), Map.empty, Seq(Money, Number).map(FixedType(_)), FixedType(Money))

  val NumToMoney = Function(FunctionName("number_to_money"), Map.empty, Seq(FixedType(Number)), FixedType(Money))
  val NumToDouble = Function(FunctionName("number_to_double"), Map.empty, Seq(FixedType(Number)), FixedType(Double))

  val MoneyToNum = Function(FunctionName("money_to_number"), Map.empty, Seq(FixedType(Money)), FixedType(Number))
  val DoubleToNum = Function(FunctionName("double_to_number"), Map.empty, Seq(FixedType(Double)), FixedType(Number))

  val TextToFixedTS = Function(FunctionName("text_to_fixed_ts"), Map.empty, Seq(FixedType(Text)), FixedType(FixedTimestamp))
  val TextToFloatTS = Function(FunctionName("text_to_float_ts"), Map.empty, Seq(FixedType(Text)), FixedType(FloatingTimestamp))

  val Avg = Function[Type](FunctionName("avg"), Map("a" -> Set(Number, Money, Double)), Seq(VariableType("a")), VariableType("a"))

  type Type = Types.Value
  case class ValueOfType(typ: Type) extends Typable[Type]

  def main(args: Array[String]) {
    val r = new FunctionCallTypechecker[Type] {
      val typeParameterUniverse = realTypes

      val implicits = Map(
        NumberLiteral -> Map(
          Money -> NumToMoney,
          Double -> NumToDouble),
        TextFixedTSLiteral -> Map(
          FixedTimestamp -> TextToFixedTS),
        TextFloatingTSLiteral -> Map(
          FloatingTimestamp -> TextToFloatTS),
        Money -> Map(
          Number -> MoneyToNum),
        Double -> Map(
          Number -> DoubleToNum))

      def implicitConversions(from: Type, to: Type) = {
        implicits.get(from).flatMap(_.get(to))
      }
      def canBePassedToWithoutConversion(actual: Type, expected: Type) =
        actual == expected || actual == Null || (numberLiterals.contains(actual) && expected == Number) || (textLiterals.contains(actual) && expected == Text)
    }

    println(r.permute(Map('a -> List(1,2,3), 'b -> List(1,2))))

    def p(fs: Set[Function[Type]], params: Seq[Typable[Type]]) {
      r.resolveOverload(fs, params) match {
        case Ambiguous(candidates) =>
          r.disambiguateNulls(candidates, params, Null) match {
            case Some((r, f, c)) =>
              println("Disambiguated via " + r + ": " + Matched(f, c))
            case _ =>
              println(Ambiguous(candidates))
          }
        case result =>
          println(result)
      }
    }

    p(Set(TimesNumNum, TimesNumMoney, TimesMoneyNum), Seq(ValueOfType(NumberLiteral), ValueOfType(NumberLiteral)))
    p(Set(TimesNumNum, TimesNumMoney, TimesMoneyNum), Seq(ValueOfType(NumberLiteral), ValueOfType(Money)))
    p(Set(TimesNumNum, TimesNumMoney, TimesMoneyNum), Seq(ValueOfType(Number), ValueOfType(Number)))
    p(Set(TimesNumNum, TimesNumMoney, TimesMoneyNum), Seq(ValueOfType(Number), ValueOfType(Null)))
    p(Set(Avg), Seq(ValueOfType(Null)))

    val X = Function[Type](FunctionName("x"), Map("a" -> Set(Number, Money, Double, Text)), Seq(VariableType("a"), VariableType("a")), FixedType(Text))
    println(r.evaluateCandidate(X, Seq(ValueOfType(Number), ValueOfType(Text))))
    p(Set(X), Seq(ValueOfType(Number), ValueOfType(Text)))

    val Y = Function[Type](FunctionName("x"), Map("a" -> Set(Number, Money)), Seq(FixedType(Text), FixedType(FloatingTimestamp), VariableType("a")), FixedType(Text))
    p(Set(Y), Seq(ValueOfType(Text), ValueOfType(FloatingTimestamp), ValueOfType(Null)))
  }
}
