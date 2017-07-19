package com.socrata.soql.functions

import java.lang.reflect.Modifier

import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.types._

sealed abstract class SoQLFunctions

object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  import SoQLTypeClasses.{Ordered, Equatable, NumLike, RealNumLike, GeospatialLike}
  private val AllTypes = SoQLType.typesByName.values.toSet

  // helpers to guide type inference (specifically forces SoQLType to be inferred)
  private def mf(identity: String, name: FunctionName, params: Seq[SoQLType], varargs: Seq[SoQLType], result: SoQLType, isAggregate: Boolean = false) =
    new MonomorphicFunction(identity, name, params, varargs, result, isAggregate = isAggregate).function
  private def f(identity: String, name: FunctionName, constraints: Map[String, Set[SoQLType]], params: Seq[TypeLike[SoQLType]], varargs: Seq[TypeLike[SoQLType]], result: TypeLike[SoQLType], isAggregate: Boolean = false) =
    Function(identity, name, constraints, params, varargs, result, isAggregate = isAggregate)

  val TextToFixedTimestamp = mf("text to fixed timestamp", SpecialFunctions.Cast(SoQLFixedTimestamp.name), Seq(SoQLText), Seq.empty, SoQLFixedTimestamp)
  val TextToFloatingTimestamp = mf("text to floating timestamp", SpecialFunctions.Cast(SoQLFloatingTimestamp.name), Seq(SoQLText), Seq.empty, SoQLFloatingTimestamp)
  val TextToDate = mf("text to date", SpecialFunctions.Cast(SoQLDate.name), Seq(SoQLText), Seq.empty, SoQLDate)
  val TextToTime = mf("text to time", SpecialFunctions.Cast(SoQLTime.name), Seq(SoQLText), Seq.empty, SoQLTime)
  val TextToPoint = mf("text to point", SpecialFunctions.Cast(SoQLPoint.name), Seq(SoQLText), Seq.empty, SoQLPoint)
  val TextToMultiPoint = mf("text to multi point", SpecialFunctions.Cast(SoQLMultiPoint.name), Seq(SoQLText), Seq.empty, SoQLMultiPoint)
  val TextToLine = mf("text to line", SpecialFunctions.Cast(SoQLLine.name), Seq(SoQLText), Seq.empty, SoQLLine)
  val TextToMultiLine = mf("text to multi line", SpecialFunctions.Cast(SoQLMultiLine.name), Seq(SoQLText), Seq.empty, SoQLMultiLine)
  val TextToPolygon = mf("text to polygon", SpecialFunctions.Cast(SoQLPolygon.name), Seq(SoQLText), Seq.empty, SoQLPolygon)
  val TextToMultiPolygon = mf("text to multi polygon", SpecialFunctions.Cast(SoQLMultiPolygon.name), Seq(SoQLText), Seq.empty, SoQLMultiPolygon)
  val TextToBlob = mf("text to blob", SpecialFunctions.Cast(SoQLBlob.name), Seq(SoQLText), Seq.empty, SoQLBlob)
  val TextToLocation = mf("text to location", SpecialFunctions.Cast(SoQLLocation.name), Seq(SoQLText), Seq.empty, SoQLLocation)
  val TextToPhone = mf("text to phone", SpecialFunctions.Cast(SoQLPhone.name), Seq(SoQLText), Seq.empty, SoQLPhone)
  val TextToUrl = mf("text to url", SpecialFunctions.Cast(SoQLUrl.name), Seq(SoQLText), Seq.empty, SoQLUrl)

  val Concat = f("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLText))
  val Gte = f(">=", SpecialFunctions.Operator(">="), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Gt = f(">", SpecialFunctions.Operator(">"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Lt = f("<", SpecialFunctions.Operator("<"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Lte = f("<=", SpecialFunctions.Operator("<="), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val Eq = f("=", SpecialFunctions.Operator("="), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val EqEq = Eq.copy(identity = "==", name = SpecialFunctions.Operator("=="))
  val Neq = f("<>", SpecialFunctions.Operator("<>"), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val BangEq = Neq.copy(identity = "!=", name = SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = f("within_circle", FunctionName("within_circle"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = f("within_box", FunctionName("within_box"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val WithinPolygon = f("within_polygon", FunctionName("within_polygon"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val Extent = f("extent", FunctionName("extent"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
  val ConcaveHull = f("concave_hull", FunctionName("concave_hull"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
  val ConvexHull = Function("convex_hull", FunctionName("convex_hull"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
  val Intersects = f("intersects", FunctionName("intersects"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val DistanceInMeters = f("distance_in_meters", FunctionName("distance_in_meters"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLNumber))
  val GeoMakeValid = f("geo_make_valid", FunctionName("geo_make_valid"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, VariableType("a"))

  // multi to multi conversion
  val GeoMultiPolygonFromMultiPolygon = mf("geo_multi_mpg_mpg", FunctionName("geo_multi"), Seq(SoQLMultiPolygon), Seq.empty, SoQLMultiPolygon)
  val GeoMultiLineFromMultiLine = mf("geo_multi_mln_mln", FunctionName("geo_multi"), Seq(SoQLMultiLine), Seq.empty, SoQLMultiLine)
  val GeoMultiPointFromMultiPoint = mf("geo_multi_mpt_mpt", FunctionName("geo_multi"), Seq(SoQLMultiPoint), Seq.empty, SoQLMultiPoint)

  // geo to multi geo conversion
  val GeoMultiPolygonFromPolygon = mf("geo_multi_mpg_pg", FunctionName("geo_multi"), Seq(SoQLPolygon), Seq.empty, SoQLMultiPolygon)
  val GeoMultiLineFromLine = mf("geo_multi_mln_ln", FunctionName("geo_multi"), Seq(SoQLLine), Seq.empty, SoQLMultiLine)
  val GeoMultiPointFromPoint = mf("geo_multi_mpt_pt", FunctionName("geo_multi"), Seq(SoQLPoint), Seq.empty, SoQLMultiPoint)

  val NumberOfPoints = f("num_points", FunctionName("num_points"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLNumber))
  val Simplify = f("simplify", FunctionName("simplify"), Map("a" -> GeospatialLike, "b" -> NumLike),
                          Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val SimplifyPreserveTopology =
    f("simplify_preserve_topology",
             FunctionName("simplify_preserve_topology"),
             Map("a" -> GeospatialLike, "b" -> NumLike),
             Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val SnapToGrid = f("snap_to_grid", FunctionName("snap_to_grid"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
  val CuratedRegionTest = f("curated_region_test", FunctionName("curated_region_test"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLText))
  val VisibleAt = f("visible_at",
                           FunctionName("visible_at"),
                           Map("a" -> GeospatialLike, "b" -> RealNumLike),
                           Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
  val IsEmpty = f("is_empty",
                         FunctionName("is_empty"),
                         Map("a" -> GeospatialLike),
                         Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))


  val IsNull = f("is null", SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val IsNotNull = f("is not null", SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))

  val Between = f("between", SpecialFunctions.Between, Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val NotBetween = f("not between", SpecialFunctions.NotBetween, Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))

  val Min = f("min", FunctionName("min"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Max = f("max", FunctionName("max"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val CountStar = mf("count(*)", SpecialFunctions.StarFunc("count"), Seq(), Seq.empty, SoQLNumber, isAggregate = true)
  val Count = f("count", FunctionName("count"), Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLNumber), isAggregate = true)
  val Sum = f("sum", FunctionName("sum"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Avg = f("avg", FunctionName("avg"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Median = f("median", FunctionName("median"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val StddevPop = f("stddev_pop", FunctionName("stddev_pop"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val StddevSamp = f("stddev_samp", FunctionName("stddev_samp"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)

  val UnaryPlus = f("unary +", SpecialFunctions.Operator("+"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))
  val UnaryMinus = f("unary -", SpecialFunctions.Operator("-"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))

  val SignedMagnitude10 = f("signed_magnitude_10", FunctionName("signed_magnitude_10"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))
  val SignedMagnitudeLinear = f("signed_magnitude_linear", FunctionName("signed_magnitude_linear"), Map("a" -> NumLike, "b" -> NumLike), Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))

  val BinaryPlus = f("+", SpecialFunctions.Operator("+"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))
  val BinaryMinus = f("-", SpecialFunctions.Operator("-"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))

  val TimesNumNum = mf("*NN", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)
  val TimesDoubleDouble = mf("*DD", SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)
  val TimesNumMoney = mf("*NM", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), Seq.empty, SoQLMoney)
  val TimesMoneyNum = mf("*MN", SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney)

  val DivNumNum = mf("/NN", SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)
  val DivDoubleDouble = mf("/DD", SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)
  val DivMoneyNum = mf("/MN", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney)
  val DivMoneyMoney = mf("/MM", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), Seq.empty, SoQLNumber)

  val ExpNumNum = mf("^NN", SpecialFunctions.Operator("^"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)
  val ExpDoubleDouble = mf("^DD", SpecialFunctions.Operator("^"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)

  val ModNumNum = mf("%NN", SpecialFunctions.Operator("%"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)
  val ModDoubleDouble = mf("%DD", SpecialFunctions.Operator("%"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)
  val ModMoneyNum = mf("%MN", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney)
  val ModMoneyMoney = mf("%MM", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLMoney), Seq.empty, SoQLNumber)

  val Ceiling = f("ceil", FunctionName("ceil"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))
  val Floor = f("floor", FunctionName("floor"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))

  val NumberToMoney = mf("number to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLNumber), Seq.empty, SoQLMoney)
  val NumberToDouble = mf("number to double", SpecialFunctions.Cast(SoQLDouble.name), Seq(SoQLNumber), Seq.empty, SoQLDouble)

  val And = mf("and", SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), Seq.empty, SoQLBoolean)
  val Or = mf("or", SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), Seq.empty, SoQLBoolean)
  val Not = mf("not", SpecialFunctions.Operator("not"), Seq(SoQLBoolean), Seq.empty, SoQLBoolean)

  val In = f("in", SpecialFunctions.In, Map.empty, Seq(VariableType("a")), Seq(VariableType("a")), FixedType(SoQLBoolean))
  val NotIn = f("not in", SpecialFunctions.NotIn, Map.empty, Seq(VariableType("a")), Seq(VariableType("a")), FixedType(SoQLBoolean))

  val Like = mf("like", SpecialFunctions.Like, Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)
  val NotLike = mf("not like", SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)

  val Contains = mf("contains", FunctionName("contains"), Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)
  val StartsWith = mf("starts_with", FunctionName("starts_with"), Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)
  val Lower = mf("lower", FunctionName("lower"), Seq(SoQLText), Seq.empty, SoQLText)
  val Upper = mf("upper", FunctionName("upper"), Seq(SoQLText), Seq.empty, SoQLText)

  val FloatingTimeStampTruncYmd = mf("floating timestamp trunc day", FunctionName("date_trunc_ymd"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp)
  val FloatingTimeStampTruncYm = mf("floating timestamp trunc month", FunctionName("date_trunc_ym"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp)
  val FloatingTimeStampTruncY = mf("floating timestamp trunc year", FunctionName("date_trunc_y"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp)

  val FloatingTimeStampExtractHh = mf("floating timestamp extract hour", FunctionName("date_extract_hh"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampExtractDow = mf("floating timestamp extract day of week", FunctionName("date_extract_dow"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampExtractWoy = mf("floating timestamp extract week of year", FunctionName("date_extract_woy"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)

  // property notation for timestamp - timestamp.field
  val FloatingTimeStampDotYear = mf("floating_timestamp_year", FunctionName("floating_timestamp_year"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotMonth = mf("floating_timestamp_month", FunctionName("floating_timestamp_month"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotDay = mf("floating_timestamp_day", FunctionName("floating_timestamp_day"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotHour = mf("floating_timestamp_hour", FunctionName("floating_timestamp_hour"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotMinute = mf("floating_timestamp_minute", FunctionName("floating_timestamp_minute"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotSecond = mf("floating_timestamp_second", FunctionName("floating_timestamp_second"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotWeekOfYear = mf("floating_timestamp_week_of_year", FunctionName("floating_timestamp_week_of_year"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)
  val FloatingTimeStampDotDayOfWeek = mf("floating_timestamp_day_of_week", FunctionName("floating_timestamp_day_of_week"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)

  val castIdentities = for ((n, t) <- SoQLType.typesByName.toSeq) yield {
    f(n.caseFolded + "::" + n.caseFolded, SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), Seq.empty, FixedType(t))
  }

  val NumberToText = mf("number to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), Seq.empty, SoQLText)
  val TextToNumber = mf("text to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), Seq.empty, SoQLNumber)
  val TextToMoney = mf("text to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLText), Seq.empty, SoQLMoney)

  val TextToBool = mf("text to boolean", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLText), Seq.empty, SoQLBoolean)
  val BoolToText = mf("boolean to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLBoolean), Seq.empty, SoQLText)

  val Prop = mf(".", SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), Seq.empty, SoQLJson)
  val Index = mf("[]", SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), Seq.empty, SoQLJson)
  val JsonProp = mf(".J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), Seq.empty, SoQLJson)
  val JsonIndex = mf("[]J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), Seq.empty, SoQLJson)

  // Cannot use property subscript syntax (loc.prop) to access properties of different types - number, text, point. Use cast syntax (loc::prop) instead
  val LocationToPoint = mf("loc to point", SpecialFunctions.Cast(SoQLPoint.name), Seq(SoQLLocation), Seq.empty, SoQLPoint)
  val LocationToLatitude = mf("location_latitude", FunctionName("location_latitude"), Seq(SoQLLocation), Seq.empty, SoQLNumber)
  val LocationToLongitude = mf("location_longitude", FunctionName("location_longitude"), Seq(SoQLLocation), Seq.empty, SoQLNumber)
  val LocationToAddress = mf("location_human_address", FunctionName("location_human_address"), Seq(SoQLLocation), Seq.empty, SoQLText)

  val LocationWithinCircle = f("location_within_circle", FunctionName("within_circle"),
    Map("a" -> RealNumLike),
    Seq(FixedType(SoQLLocation), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(SoQLBoolean))
  val LocationWithinBox = f("location_within_box", FunctionName("within_box"),
    Map("a" -> RealNumLike),
    Seq(FixedType(SoQLLocation), VariableType("a"), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(SoQLBoolean))
  val LocationWithinPolygon = f("location_within_polygon", FunctionName("within_polygon"),
    Map("a" -> GeospatialLike),
    Seq(FixedType(SoQLLocation), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))
  val LocationDistanceInMeters = f("location_distance_in_meters", FunctionName("distance_in_meters"),
    Map("a" -> GeospatialLike),
    Seq(FixedType(SoQLLocation), VariableType("a")), Seq.empty, FixedType(SoQLNumber))

  val Location = f("location", FunctionName("location"), Map.empty,
    Seq(FixedType(SoQLPoint), FixedType(SoQLText), FixedType(SoQLText), FixedType(SoQLText), FixedType(SoQLText)), Seq.empty,
    FixedType(SoQLLocation))

  /**
   * human_address should return a string address in json object form
   * like { "address": "101 Main St", "city": "Seattle", "state": "WA", "zip": "98104" }
   */
  val HumanAddress = mf("human_address", FunctionName("human_address"),
    Seq(SoQLText, SoQLText, SoQLText, SoQLText), Seq.empty, SoQLText)

  val PointToLatitude = mf("point_latitude", FunctionName("point_latitude"), Seq(SoQLPoint), Seq.empty, SoQLNumber)
  val PointToLongitude = mf("point_longitude", FunctionName("point_longitude"), Seq(SoQLPoint), Seq.empty, SoQLNumber)

  val PhoneToPhoneNumber = mf("phone_phone_number", FunctionName("phone_phone_number"), Seq(SoQLPhone), Seq.empty, SoQLText)
  val PhoneToPhoneType = mf("phone_phone_type", FunctionName("phone_phone_type"), Seq(SoQLPhone), Seq.empty, SoQLText)

  val Phone = f("phone", FunctionName("phone"), Map.empty,
    Seq(FixedType(SoQLText), FixedType(SoQLText)), Seq.empty,
    FixedType(SoQLPhone))

  val UrlToUrl = mf("url_url", FunctionName("url_url"), Seq(SoQLUrl), Seq.empty, SoQLText)
  val UrlToDescription = mf("url_description", FunctionName("url_description"), Seq(SoQLUrl), Seq.empty, SoQLText)

  val Url = f("url", FunctionName("url"), Map.empty,
    Seq(FixedType(SoQLText), FixedType(SoQLText)), Seq.empty,
    FixedType(SoQLUrl))

  val JsonToText = mf("json to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), Seq.empty, SoQLText)
  val JsonToNumber = mf("json to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), Seq.empty, SoQLNumber)
  val JsonToBool = mf("json to bool", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), Seq.empty, SoQLBoolean)
  val JsonToObject = mf("json to obj", SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), Seq.empty, SoQLObject)
  val JsonToArray = mf("json to array", SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), Seq.empty, SoQLArray)

  val TextToRowIdentifier = mf("text to rid", SpecialFunctions.Cast(SoQLID.name), Seq(SoQLText), Seq.empty, SoQLID)
  val TextToRowVersion = mf("text to rowver", SpecialFunctions.Cast(SoQLVersion.name), Seq(SoQLText), Seq.empty, SoQLVersion)

  val Case = f("case", FunctionName("case"),
    Map("a" -> AllTypes),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
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

  val nAdicFunctionsByNameThenArity: Map[FunctionName, Map[Int, Set[Function[SoQLType]]]] =
    nAdicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.toSet).toMap
    }.toMap

  val variadicFunctionsByNameThenMinArity: Map[FunctionName, Map[Int, Set[Function[SoQLType]]]] =
    variadicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.toSet).toMap
    }.toMap
}
