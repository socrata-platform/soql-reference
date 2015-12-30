package com.socrata.soql.functions

import java.lang.reflect.Modifier

import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.types._

sealed abstract class SoQLFunctions

object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  private def typeclass(f: SoQLType => Boolean): Any => Boolean = { // design compromises rear their ugly head
    case x: SoQLType => f(x)
    case _ => false
  }
  private val Ordered = typeclass(SoQLTypeClasses.Ordered)
  private val Equatable = typeclass(SoQLTypeClasses.Equatable)
  private val NumLike = typeclass(SoQLTypeClasses.NumLike)
  private val RealNumLike = typeclass(SoQLTypeClasses.RealNumLike)
  private val GeospatialLike = typeclass(SoQLTypeClasses.GeospatialLike)
  private val AllTypes = SoQLType.typesByName.values.toSet[Any]

  val TextToFixedTimestamp = new MonomorphicFunction("text to fixed timestamp", SpecialFunctions.Cast(SoQLFixedTimestamp.name), Seq(SoQLText), Seq.empty, SoQLFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction("text to floating timestamp", SpecialFunctions.Cast(SoQLFloatingTimestamp.name), Seq(SoQLText), Seq.empty, SoQLFloatingTimestamp).function
  val TextToDate = new MonomorphicFunction("text to date", SpecialFunctions.Cast(SoQLDate.name), Seq(SoQLText), Seq.empty, SoQLDate).function
  val TextToTime = new MonomorphicFunction("text to time", SpecialFunctions.Cast(SoQLTime.name), Seq(SoQLText), Seq.empty, SoQLTime).function
  val TextToPoint = new MonomorphicFunction("text to point", SpecialFunctions.Cast(SoQLPoint.name), Seq(SoQLText), Seq.empty, SoQLPoint).function
  val TextToMultiPoint = new MonomorphicFunction("text to multi point", SpecialFunctions.Cast(SoQLMultiPoint.name), Seq(SoQLText), Seq.empty, SoQLMultiPoint).function
  val TextToLine = new MonomorphicFunction("text to line", SpecialFunctions.Cast(SoQLLine.name), Seq(SoQLText), Seq.empty, SoQLLine).function
  val TextToMultiLine = new MonomorphicFunction("text to multi line", SpecialFunctions.Cast(SoQLMultiLine.name), Seq(SoQLText), Seq.empty, SoQLMultiLine).function
  val TextToPolygon = new MonomorphicFunction("text to polygon", SpecialFunctions.Cast(SoQLPolygon.name), Seq(SoQLText), Seq.empty, SoQLPolygon).function
  val TextToMultiPolygon = new MonomorphicFunction("text to multi polygon", SpecialFunctions.Cast(SoQLMultiPolygon.name), Seq(SoQLText), Seq.empty, SoQLMultiPolygon).function
  val TextToBlob = new MonomorphicFunction("text to blob", SpecialFunctions.Cast(SoQLBlob.name), Seq(SoQLText), Seq.empty, SoQLBlob).function
  val TextToLocation = new MonomorphicFunction("text to location", SpecialFunctions.Cast(SoQLLocation.name), Seq(SoQLText), Seq.empty, SoQLLocation).function

  val Concat = Function("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLText))
  val Gte = Function(">=", SpecialFunctions.Operator(">="), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Gt = Function(">", SpecialFunctions.Operator(">"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Lt = Function("<", SpecialFunctions.Operator("<"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Lte = Function("<=", SpecialFunctions.Operator("<="), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Eq = Function("=", SpecialFunctions.Operator("="), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val EqEq = Eq.copy(identity = "==", name = SpecialFunctions.Operator("=="))
  val Neq = Function("<>", SpecialFunctions.Operator("<>"), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val BangEq = Neq.copy(identity = "!=", name = SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = Function("within_circle", FunctionName("within_circle"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = Function("within_box", FunctionName("within_box"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val WithinPolygon = Function("within_polygon", FunctionName("within_polygon"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val Extent = Function("extent", FunctionName("extent"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
  val ConcaveHull = Function("concave_hull", FunctionName("concave_hull"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
  val ConvexHull = Function("convex_hull", FunctionName("convex_hull"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
  val Intersects = Function("intersects", FunctionName("intersects"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val DistanceInMeters = Function("distance_in_meters", FunctionName("distance_in_meters"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLNumber))
  val Simplify = Function("simplify", FunctionName("simplify"), Map("a" -> GeospatialLike, "b" -> NumLike),
                          Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val SimplifyPreserveTopology =
    Function("simplify_preserve_topology",
             FunctionName("simplify_preserve_topology"),
             Map("a" -> GeospatialLike, "b" -> NumLike),
             Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val SnapToGrid = Function("snap_to_grid", FunctionName("snap_to_grid"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val CuratedRegionTest = Function("curated_region_test", FunctionName("curated_region_test"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLText))
  val VisibleAt = Function("visible_at",
                           FunctionName("visible_at"),
                           Map("a" -> GeospatialLike, "b" -> RealNumLike),
                           Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val IsEmpty = Function("is_empty",
                         FunctionName("is_empty"),
                         Map("a" -> GeospatialLike),
                         Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))


  val IsNull = Function("is null", SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val IsNotNull = Function("is not null", SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))

  val Between = Function("between", SpecialFunctions.Between, Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val NotBetween = Function("not between", SpecialFunctions.NotBetween, Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))

  val Min = Function("min", FunctionName("min"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Max = Function("max", FunctionName("max"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val CountStar = new MonomorphicFunction("count(*)", SpecialFunctions.StarFunc("count"), Seq(), Seq.empty, SoQLNumber, isAggregate = true).function
  val Count = Function("count", FunctionName("count"), Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLNumber), isAggregate = true)
  val Sum = Function("sum", FunctionName("sum"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Avg = Function("avg", FunctionName("avg"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)

  val UnaryPlus = Function("unary +", SpecialFunctions.Operator("+"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))
  val UnaryMinus = Function("unary -", SpecialFunctions.Operator("-"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))

  val SignedMagnitude10 = Function("signed_magnitude_10", FunctionName("signed_magnitude_10"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))
  val SignedMagnitudeLinear = Function("signed_magnitude_linear", FunctionName("signed_magnitude_linear"), Map("a" -> NumLike, "b" -> NumLike), Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))

  val BinaryPlus = Function("+", SpecialFunctions.Operator("+"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))
  val BinaryMinus = Function("-", SpecialFunctions.Operator("-"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))

  val TimesNumNum = new MonomorphicFunction("*NN", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber).function
  val TimesDoubleDouble = new MonomorphicFunction("*DD", SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble).function
  val TimesNumMoney = new MonomorphicFunction("*NM", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), Seq.empty, SoQLMoney).function
  val TimesMoneyNum = new MonomorphicFunction("*MN", SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney).function

  val DivNumNum = new MonomorphicFunction("/NN", SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber).function
  val DivDoubleDouble = new MonomorphicFunction("/DD", SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble).function
  val DivMoneyNum = new MonomorphicFunction("/MN", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney).function
  val DivMoneyMoney = new MonomorphicFunction("/MM", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), Seq.empty, SoQLNumber).function

  val ExpNumNum = new MonomorphicFunction("^NN", SpecialFunctions.Operator("^"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber).function
  val ExpDoubleDouble = new MonomorphicFunction("^DD", SpecialFunctions.Operator("^"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble).function

  val ModNumNum = new MonomorphicFunction("%NN", SpecialFunctions.Operator("%"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber).function
  val ModDoubleDouble = new MonomorphicFunction("%DD", SpecialFunctions.Operator("%"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble).function
  val ModMoneyNum = new MonomorphicFunction("%MN", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney).function
  val ModMoneyMoney = new MonomorphicFunction("%MM", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLMoney), Seq.empty, SoQLNumber).function

  val NumberToMoney = new MonomorphicFunction("number to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLNumber), Seq.empty, SoQLMoney).function
  val NumberToDouble = new MonomorphicFunction("number to double", SpecialFunctions.Cast(SoQLDouble.name), Seq(SoQLNumber), Seq.empty, SoQLDouble).function

  val And = new MonomorphicFunction("and", SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), Seq.empty, SoQLBoolean).function
  val Or = new MonomorphicFunction("or", SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), Seq.empty, SoQLBoolean).function
  val Not = new MonomorphicFunction("not", SpecialFunctions.Operator("not"), Seq(SoQLBoolean), Seq.empty, SoQLBoolean).function

  val In = new Function("in", SpecialFunctions.In, Map.empty, Seq(VariableType("a")), Seq(VariableType("a")), FixedType(SoQLBoolean))
  val NotIn = new Function("not in", SpecialFunctions.NotIn, Map.empty, Seq(VariableType("a")), Seq(VariableType("a")), FixedType(SoQLBoolean))

  val Like = new MonomorphicFunction("like", SpecialFunctions.Like, Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean).function
  val NotLike = new MonomorphicFunction("not like", SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean).function

  val Contains = new MonomorphicFunction("contains", FunctionName("contains"), Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean).function
  val StartsWith = new MonomorphicFunction("starts_with", FunctionName("starts_with"), Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean).function
  val Lower = new MonomorphicFunction("lower", FunctionName("lower"), Seq(SoQLText), Seq.empty, SoQLText).function
  val Upper = new MonomorphicFunction("upper", FunctionName("upper"), Seq(SoQLText), Seq.empty, SoQLText).function

  val FloatingTimeStampTruncYmd = new MonomorphicFunction("floating timestamp trunc day", FunctionName("date_trunc_ymd"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp).function
  val FloatingTimeStampTruncYm = new MonomorphicFunction("floating timestamp trunc month", FunctionName("date_trunc_ym"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp).function
  val FloatingTimeStampTruncY = new MonomorphicFunction("floating timestamp trunc year", FunctionName("date_trunc_y"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp).function

  val castIdentities = for ((n, t) <- SoQLType.typesByName.toSeq) yield {
    Function(n.caseFolded + "::" + n.caseFolded, SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), Seq.empty, FixedType(t))
  }

  val NumberToText = new MonomorphicFunction("number to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), Seq.empty, SoQLText).function
  val TextToNumber = new MonomorphicFunction("text to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), Seq.empty, SoQLNumber).function
  val TextToMoney = new MonomorphicFunction("text to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLText), Seq.empty, SoQLMoney).function

  val TextToBool = new MonomorphicFunction("text to boolean", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLText), Seq.empty, SoQLBoolean).function
  val BoolToText = new MonomorphicFunction("boolean to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLBoolean), Seq.empty, SoQLText).function

  val Prop = new MonomorphicFunction(".", SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), Seq.empty, SoQLJson).function
  val Index = new MonomorphicFunction("[]", SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), Seq.empty, SoQLJson).function
  val JsonProp = new MonomorphicFunction(".J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), Seq.empty, SoQLJson).function
  val JsonIndex = new MonomorphicFunction("[]J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), Seq.empty, SoQLJson).function

  // Cannot use property subscript syntax (loc.prop) to access properties of different types - number, text, point. Use cast syntax (loc::prop) instead
  val LocationToPoint = new MonomorphicFunction("loc to point", SpecialFunctions.Cast(SoQLPoint.name), Seq(SoQLLocation), Seq.empty, SoQLPoint).function
  val LocationToLatitude = new MonomorphicFunction("loc to latitude", FunctionName("location_latitude"), Seq(SoQLLocation), Seq.empty, SoQLNumber).function
  val LocationToLongitude = new MonomorphicFunction("loc to longitude", FunctionName("location_longitude"), Seq(SoQLLocation), Seq.empty, SoQLNumber).function
  val LocationToAddress = new MonomorphicFunction("loc to address", FunctionName("location_address"), Seq(SoQLLocation), Seq.empty, SoQLText).function

  val LocationWithinCircle = Function("location_within_circle", FunctionName("within_circle"),
    Map("a" -> RealNumLike),
    Seq(FixedType(SoQLLocation), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(SoQLBoolean))
  val LocationWithinBox = Function("location_within_box", FunctionName("within_box"),
    Map("a" -> RealNumLike),
    Seq(FixedType(SoQLLocation), VariableType("a"), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(SoQLBoolean))

  val JsonToText = new MonomorphicFunction("json to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), Seq.empty, SoQLText).function
  val JsonToNumber = new MonomorphicFunction("json to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), Seq.empty, SoQLNumber).function
  val JsonToBool = new MonomorphicFunction("json to bool", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), Seq.empty, SoQLBoolean).function
  val JsonToObject = new MonomorphicFunction("json to obj", SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), Seq.empty, SoQLObject).function
  val JsonToArray = new MonomorphicFunction("json to array", SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), Seq.empty, SoQLArray).function

  val TextToRowIdentifier = new MonomorphicFunction("text to rid", SpecialFunctions.Cast(SoQLID.name), Seq(SoQLText), Seq.empty, SoQLID).function
  val TextToRowVersion = new MonomorphicFunction("text to rowver", SpecialFunctions.Cast(SoQLVersion.name), Seq(SoQLText), Seq.empty, SoQLVersion).function

  val Case = Function("case", FunctionName("case"),
    Map("a" -> AllTypes),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    VariableType("a"))

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  val allFunctions: Seq[Function[SoQLType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
    castIdentities ++ reflectedFunctions
  }

  val functionsByIdentity = allFunctions.map { f => f.identity -> f }.toMap

  val nAdicFunctions = SoQLFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = SoQLFunctions.allFunctions.filter(_.isVariadic)

  private def analysisify(f: Function[SoQLType]): Function[SoQLAnalysisType] = f

  val nAdicFunctionsByNameThenArity: Map[FunctionName, Map[Int, Set[Function[SoQLAnalysisType]]]] =
    nAdicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap

  val variadicFunctionsByNameThenMinArity: Map[FunctionName, Map[Int, Set[Function[SoQLAnalysisType]]]] =
    variadicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap
}
