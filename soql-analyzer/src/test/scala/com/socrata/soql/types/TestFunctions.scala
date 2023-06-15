package com.socrata.soql.types

import java.lang.reflect.Modifier

import com.socrata.soql.collection._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.functions._

sealed abstract class TestFunctions

object TestFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[TestFunctions])

  // TODO: might want to narrow this down
  private val Ordered = CovariantSet.from(TestTypeInfo.typeParameterUniverse.toSet)
  private val NumLike = CovariantSet[TestType](TestNumber, TestDouble, TestMoney)
  private val RealNumLike = CovariantSet[TestType](TestNumber, TestDouble)
  private val GeospatialLike = CovariantSet[TestType](TestPoint, TestMultiPoint, TestLine, TestMultiLine, TestPolygon, TestMultiPolygon)
  private val Equatable = Ordered ++ GeospatialLike ++ CovariantSet[TestType](TestText, TestNumber)
  private val AllTypes = CovariantSet.from(TestType.typesByName.values.toSet)

  // helpers to guide type inference (specifically forces TestType to be inferred)
  private def mf(identity: String, name: FunctionName, params: Seq[TestType], varargs: Seq[TestType], result: TestType, isAggregate: Boolean = false, needsWindow: Boolean = false) =
    new MonomorphicFunction(identity, name, params, varargs, result, isAggregate = isAggregate, needsWindow = needsWindow)(Function.Doc.empty).function
  private def f(identity: String, name: FunctionName, constraints: Map[String, CovariantSet[TestType]], params: Seq[TypeLike[TestType]], varargs: Seq[TypeLike[TestType]], result: TypeLike[TestType], isAggregate: Boolean = false, needsWindow: Boolean = false) =
    Function(identity, name, constraints, params, varargs, result, isAggregate = isAggregate, needsWindow = needsWindow, Function.Doc.empty)

  val TextToLocation = mf("text to location", SpecialFunctions.Cast(TestLocation.name), Seq(TestText), Seq.empty, TestLocation)

  val Like = mf("like", SpecialFunctions.Like, Seq(TestText, TestText), Seq.empty, TestBoolean)

  val Concat = f("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(TestText))
  val Eq = f("=", SpecialFunctions.Operator("="), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))
  val Gt = f(">", SpecialFunctions.Operator(">"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))
  val Lt = f("<", SpecialFunctions.Operator("<"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))

  val Max = f("max", FunctionName("max"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Sum = f("sum", FunctionName("sum"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Avg = f("avg", FunctionName("avg"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val CountStar = mf("count(*)", SpecialFunctions.StarFunc("count"), Seq(), Seq.empty, TestNumber, isAggregate = true)

  val RowNumber = mf("row_number", FunctionName("row_number"), Seq(), Seq.empty, TestNumber, needsWindow = true)

  val Mul = f("*", SpecialFunctions.Operator("*"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"), isAggregate = false)

  val And = f("and", SpecialFunctions.Operator("and"), Map.empty, Seq(FixedType(TestBoolean), FixedType(TestBoolean)), Seq.empty, FixedType(TestBoolean), isAggregate = false)

  val SignedMagnitude10 = f("signed_magnitude_10", FunctionName("signed_magnitude_10"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))
  val SignedMagnitudeLinear = f("signed_magnitude_linear", FunctionName("signed_magnitude_linear"), Map("a" -> NumLike, "b" -> NumLike), Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))

  val NumberToMoney = mf("number to money", SpecialFunctions.Operator("to_money"), Seq(TestNumber), Seq.empty, TestMoney)
  val NumberToDouble = mf("number to double", SpecialFunctions.Operator("to_double"), Seq(TestNumber), Seq.empty, TestDouble)

  val castIdentities = for ((n, t) <- TestType.typesByName.toSeq) yield {
    f(n.caseFolded + "::" + n.caseFolded, SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), Seq.empty, FixedType(t))
  }

  val NumberToText = mf("number to text", SpecialFunctions.Cast(TestText.name), Seq(TestNumber), Seq.empty, TestText)
  val TextToNumber = mf("text to number", SpecialFunctions.Cast(TestNumber.name), Seq(TestText), Seq.empty, TestNumber)

  val Prop = mf(".", SpecialFunctions.Subscript, Seq(TestObject, TestText), Seq.empty, TestJson)
  val Index = mf("[]", SpecialFunctions.Subscript, Seq(TestArray, TestNumber), Seq.empty, TestJson)

  val JsonToText = mf("json to text", SpecialFunctions.Cast(TestText.name), Seq(TestJson), Seq.empty, TestText)
  val JsonToNumber = mf("json to Number", SpecialFunctions.Cast(TestNumber.name), Seq(TestJson), Seq.empty, TestNumber)

  val FloatingTimeStampExtractHh = mf("floating timestamp extract hour", FunctionName("date_extract_hh"), Seq(TestFloatingTimestamp), Seq.empty, TestNumber)
  val FloatingTimeStampExtractDow = mf("floating timestamp extract day of week", FunctionName("date_extract_dow"), Seq(TestFloatingTimestamp), Seq.empty, TestNumber)

  val LocationToPoint = mf("loc to point", SpecialFunctions.Cast(TestPoint.name), Seq(TestLocation), Seq.empty, TestPoint)
  val LocationToLatitude = mf("location_latitude", SpecialFunctions.Field(TestLocation.name, "latitude"), Seq(TestLocation), Seq.empty, TestNumber)
  val LocationToLongitude = mf("location_longitude", SpecialFunctions.Field(TestLocation.name, "longitude"), Seq(TestLocation), Seq.empty, TestNumber)
  val LocationToAddress = mf("location_human_address", SpecialFunctions.Field(TestLocation.name, "human_address"), Seq(TestLocation), Seq.empty, TestText)

  val LocationWithinCircle = f("location_within_circle", FunctionName("within_circle"),
    Map("a" -> RealNumLike),
    Seq(FixedType(TestLocation), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(TestBoolean))
  val LocationWithinBox = f("location_within_box", FunctionName("within_box"),
    Map("a" -> RealNumLike),
    Seq(FixedType(TestLocation), VariableType("a"), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(TestBoolean))

  val Simplify = f("simplify", FunctionName("simplify"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val CuratedRegionTest = f("curated_region_test", FunctionName("curated_region_test"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(TestText))

  val FloatingTimeStampTruncYmd = mf("floating timestamp trunc day", FunctionName("date_trunc_ymd"), Seq(TestFloatingTimestamp), Seq.empty, TestFloatingTimestamp)
  val FloatingTimeStampTruncYm = mf("floating timestamp trunc month", FunctionName("date_trunc_ym"), Seq(TestFloatingTimestamp), Seq.empty, TestFloatingTimestamp)
  val FloatingTimeStampTruncY = mf("floating timestamp trunc year", FunctionName("date_trunc_y"), Seq(TestFloatingTimestamp), Seq.empty, TestFloatingTimestamp)

  val FixedTimeStampZTruncYmd = mf("fixed timestamp z trunc day", FunctionName("datez_trunc_ymd"), Seq(TestFixedTimestamp), Seq.empty, TestFixedTimestamp)
  val FixedTimeStampZTruncYm = mf("fixed timestamp z trunc month", FunctionName("datez_trunc_ym"), Seq(TestFixedTimestamp), Seq.empty, TestFixedTimestamp)
  val FixedTimeStampZTruncY = mf("fixed timestamp z trunc year", FunctionName("datez_trunc_y"), Seq(TestFixedTimestamp), Seq.empty, TestFixedTimestamp)

  val FixedTimeStampTruncYmdAtTimeZone = mf("fixed timestamp trunc day at time zone", FunctionName("date_trunc_ymd"), Seq(TestFixedTimestamp, TestText), Seq.empty, TestFloatingTimestamp)
  val FixedTimeStampTruncYmAtTimeZone = mf("fixed timestamp trunc month at time zone", FunctionName("date_trunc_ym"), Seq(TestFixedTimestamp, TestText), Seq.empty, TestFloatingTimestamp)
  val FixedTimeStampTruncYAtTimeZone = mf("fixed timestamp trunc year at time zone", FunctionName("date_trunc_y"), Seq(TestFixedTimestamp, TestText), Seq.empty, TestFloatingTimestamp)

  val ToFloatingTimestamp = mf("to floating timestamp", FunctionName("to_floating_timestamp"), Seq(TestFixedTimestamp, TestText), Seq.empty, TestFloatingTimestamp)

  val Lower = mf("lower", FunctionName("lower"), Seq(TestText), Seq.empty, TestText)
  val Upper = mf("upper", FunctionName("upper"), Seq(TestText), Seq.empty, TestText)

  val Case = f("case", FunctionName("case"),
    Map("a" -> AllTypes),
    Seq(FixedType(TestBoolean), VariableType("a")),
    Seq(FixedType(TestBoolean), VariableType("a")),
    VariableType("a"))

  val Coalesce = f("coalesce", FunctionName("coalesce"),
    Map.empty,
    Seq(VariableType("a")),
    Seq(VariableType("a")),
    VariableType("a"))

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  for (potentialAccessor <- potentialAccessors) {
    assert(potentialAccessor.getReturnType != classOf[MonomorphicFunction[_]])
  }

  val allFunctions: Seq[Function[TestType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[TestType]]
    castIdentities ++ reflectedFunctions
  }

  val functionsByIdentity = allFunctions.foldLeft(Map.empty[String, Function[TestType]]) { (acc, func) =>
    acc + (func.identity -> func)
  }
  assert(allFunctions.size == functionsByIdentity.size)

  val nAdicFunctions = TestFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = TestFunctions.allFunctions.filter(_.isVariadic)
  val windowFunctions = TestFunctions.allFunctions.filter(_.needsWindow)

  locally {
    val sharedNames = nAdicFunctions.map(_.name).toSet intersect variadicFunctions.map(_.name).toSet
    assert(sharedNames.isEmpty)
  }

  val nAdicFunctionsByNameThenArity = nAdicFunctions.groupBy(_.name).view.mapValues { fs =>
    fs.groupBy(_.minArity).view.mapValues(_.toSet).toMap
  }.toMap

  val variadicFunctionsByNameThenMinArity = variadicFunctions.groupBy(_.name).view.mapValues { fs =>
    fs.groupBy(_.minArity).view.mapValues(_.toSet).toMap
  }.toMap
}
